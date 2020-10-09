extern crate clap;
use clap::{App, Arg, SubCommand, AppSettings};

use std::env;
use std::fs;
use std::fs::File;
use std::fs::metadata;
use std::path::Path;
use std::io;
use std::io::Read;
use std::io::ErrorKind;

use scan_dir::ScanDir;
use url::Url;

use rusoto_core::credential::StaticProvider;
use rusoto_core::Region;
use rusoto_s3::{
    GetObjectRequest,
    ListObjectsV2Request,
    PutObjectRequest,
    S3Client,
    S3,
};

struct SyncClient {
    client: S3Client
}

impl SyncClient {
    pub fn new_from_env() -> Self {

        let region = Region::Custom {
            name: env::var("AWS_REGION").unwrap().into(),
            endpoint: env::var("AWS_HOST").unwrap().into(),
        };
        let credentials = StaticProvider::new(
            env::var("AWS_ACCESS_KEY_ID").unwrap(),
            env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
            None,
            None,
        );

        let client = S3Client::new_with(
            rusoto_core::request::HttpClient::new().expect("Failed to creat HTTP client"),
            credentials,
            region.clone(),
        );

        SyncClient {client: client}
    }
}

pub fn checkfile(file_name: &Path, expected_hash: &str) -> bool {
    let mut file = File::open(file_name).unwrap();
    let mut md5_context = md5::Context::new();
    io::copy(&mut file, &mut md5_context).unwrap();
    let hash = md5_context.compute();
    let hash = base16::encode_lower(&hash.0);
    return hash == expected_hash;
}


#[tokio::main]
async fn main() -> std::io::Result<()> {
    let matches = App::new("musync")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version("0.5")
        .about("s3 sync tools in rust, provide credentials by setting envs: AWS_REGION, AWS_HOST, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        // .help("provide credentials by setting envs: AWS_REGION, AWS_HOST, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        .subcommand(
            SubCommand::with_name("ls")
                .about("List bucket or objects")
                .arg(Arg::with_name("uri").help("s3 bucket or object uri")),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get file from bucket")
                .arg(Arg::with_name("source").help("s3 object uri"))
                .arg(Arg::with_name("dest").help("local path")),
        )
        .subcommand(
            SubCommand::with_name("put")
                .about("Put file into bucket")
                .arg(Arg::with_name("source").help("local file"))
                .arg(Arg::with_name("dest").help("remove path")),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Delete file from bucket")
                .arg(Arg::with_name("uri").help("s3 object uri")),
        )
        .subcommand(
            SubCommand::with_name("info")
                .about("Print information about Buckets or Files")
                .arg(Arg::with_name("uri").help("s3 object uri")),
        )
        .subcommand(
            SubCommand::with_name("sync")
                .about("Synchronize a directory tree to S3")
                .arg(Arg::with_name("source").help("sync source"))
                .arg(Arg::with_name("dest").help("sync destination")),
        )
        .subcommand(
            SubCommand::with_name("msync")
                .about("Synchronize multiple directories to S3")
                .arg(
                    Arg::with_name("source_dest")
                        .multiple(true)
                        .help("source and dest pair"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("ls", Some(sub)) => {
            let uri = &sub.args["uri"].vals[0].to_str().unwrap();
            list_bucket(uri).await.expect("Failed to list objects");
        },
        ("get", Some(sub)) => {
            let source = &sub.args["source"].vals[0].to_str().unwrap();
            let dest = &sub.args["dest"].vals[0].to_str().unwrap();
            get_object(source, dest).await.expect("Failed to get object");
        }
        ("put", Some(sub)) => {
            let source = &sub.args["source"].vals[0].to_str().unwrap();
            let dest = &sub.args["dest"].vals[0].to_str().unwrap();
            put_object(source, dest).await.expect("Failed to put object");
        }
        ("sync", Some(sub)) => {
            let source = &sub.args["source"].vals[0].to_str().unwrap();
            let dest = &sub.args["dest"].vals[0].to_str().unwrap();
            sync_dir(source, dest).await.expect("Failed to sync objects");
        }
        _ => {
            println!("{:?}", matches);
        }

    }

    Ok(())
}

async fn list_bucket(uri: &str) -> Result<(), url::ParseError> {
    if !uri.starts_with("s3://") {
        panic!("{:?} is not s3 path", uri);
    }

    let parsed = Url::parse(uri).expect("invalid url");
    let bucket_name = parsed.host().unwrap();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").to_string();
    let client = SyncClient::new_from_env().client;

    let req = ListObjectsV2Request {
        bucket: bucket_name.to_string().clone(),
        prefix: Some(path.clone()),
        ..Default::default()
    };
    let resp = &client
        .list_objects_v2(req)
        .await
        .expect("failed to list objects v2");
    println!("{:?}", resp);

    if let Some(common_prefixes) = &resp.common_prefixes {
        for comm in common_prefixes {
            println!("dir s3://{}/{}", bucket_name, comm.prefix.as_ref().unwrap());
        }
    }

    if let Some(objs) = &resp.contents {
        for obj in objs {
            let e_tag = obj.e_tag.as_ref().unwrap();
            let e_tag = e_tag.trim_start_matches("\"").trim_end_matches("\"");
            println!(
                "{} {} {} s3://{}/{}",
                obj.last_modified.as_ref().unwrap(),
                e_tag,
                obj.size.unwrap(),
                bucket_name,
                obj.key.as_ref().unwrap()
            );
        }
    }

    Ok(())
}

async fn get_object(source: &str, dest: &str) -> Result<(), url::ParseError> {
    if !source.starts_with("s3://") {
        panic!("{:?} is not s3 path", source);
    }

    let parsed = Url::parse(source)?;
    let bucket_name = parsed.host().unwrap().to_string();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").to_string();
    let local_file_path = dest;
    let client = SyncClient::new_from_env().client;

    let local_file_dir = Path::new(&local_file_path).parent().unwrap();
    let mut output_file = match tokio::fs::File::create(&local_file_path).await {
        Ok(file) => file,
        Err(error) => match error.kind() {
            ErrorKind::NotFound => {
                fs::create_dir_all(&local_file_dir).expect("create fail");
                tokio::fs::File::create(&local_file_path).await.unwrap()
            }
            other_error => panic!("Problem opening the file: {:?}", other_error),
        },
    };

    let get_obj_req = GetObjectRequest {
        bucket: bucket_name.clone(),
        key: path.clone(),
        ..Default::default()
    };
    let resp = client.get_object(get_obj_req).await;
    let mut result = resp.unwrap();
    println!("{:?}", &result);

    let stream = result.body.take().expect("no body");
    println!("{:?}", &stream);
    println!("{:?}", &output_file);
    tokio::io::copy(&mut stream.into_async_read(), &mut output_file)
        .await
        .expect("copy fail");

    Ok(())
}

async fn put_object(source: &str, dest: &str) -> Result<(), url::ParseError> {
    let client = SyncClient::new_from_env().client;

    if !dest.starts_with("s3://") {
        panic!("{:?} is not s3 path", dest);
    }

    let parsed = Url::parse(dest)?;
    let bucket_name = parsed.host().unwrap().to_string();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").trim_end_matches("/");
    let local_file_path = source;

    let md = metadata(&local_file_path).unwrap();
    if !md.is_file() {
        panic!("{:?} is not file", local_file_path);
    }
    let mut file = File::open(&local_file_path).unwrap();
    let mut buffer = vec![0; md.len() as usize];
    file.read(&mut buffer).expect("buffer overflow");
    // todo : streaming upload

    let put_request = PutObjectRequest {
        bucket: bucket_name.clone(),
        key: path.to_string(),
        body: Some(buffer.into()),
        ..Default::default()
    };

    client
        .put_object(put_request)
        .await
        .expect("Failed to put test object");

    Ok(())
}

async fn sync_dir(source: &str, dest: &str) -> Result<(), url::ParseError> {
    let client = SyncClient::new_from_env().client;

    if source.starts_with("s3://") && dest.starts_with("s3://") {
        println!("both s3 address not supported");
    } else if source.starts_with("s3://") {
        println!("downloading");

        let parsed = Url::parse(source)?;
        let bucket_name = parsed.host().unwrap().to_string();
        let raw_path = parsed.path();
        let path = raw_path.trim_start_matches("/");
        let local_dir = dest;

        let list_obj_req_v2 = ListObjectsV2Request {
            bucket: bucket_name.clone(),
            prefix: Some(path.to_string()),
            ..Default::default()
        };
        let resp = client
            .list_objects_v2(list_obj_req_v2)
            .await
            .expect("failed to list objects");
        println!("");

        if let Some(objs) = &resp.contents {
            for obj in objs {
                let e_tag = obj.e_tag.as_ref().unwrap();
                let e_tag = e_tag.trim_start_matches("\"").trim_end_matches("\"");
                let key = obj.key.as_ref().unwrap();
                println!(
                    "{} {} {} s3://{}/{}",
                    obj.last_modified.as_ref().unwrap(),
                    e_tag,
                    obj.size.unwrap(),
                    bucket_name,
                    key
                );
                let sub_path = obj.key.as_ref().unwrap().trim_start_matches(path);
                let local_file_path = if raw_path.ends_with("/") {
                    Path::new(local_dir).join(sub_path)
                } else {
                    let mid_path = Path::new(path).file_name().unwrap().to_str().unwrap();
                    Path::new(local_dir).join([mid_path, sub_path].concat())
                };
                let local_file_dir = Path::new(&local_file_path).parent().unwrap();

                if Path::new(&local_file_path).exists() && checkfile(&local_file_path, e_tag) {
                    println!("skip {:?} -> {:?}", obj.key, local_file_path);
                } else {
                    let mut output_file = match tokio::fs::File::create(&local_file_path).await {
                        Ok(file) => file,
                        Err(error) => match error.kind() {
                            ErrorKind::NotFound => {
                                fs::create_dir_all(&local_file_dir).expect("create fail");
                                tokio::fs::File::create(&local_file_path).await.unwrap()
                            }
                            other_error => panic!("Problem opening the file: {:?}", other_error),
                        },
                    };

                    let get_obj_req = GetObjectRequest {
                        bucket: bucket_name.clone(),
                        key: key.to_string(),
                        ..Default::default()
                    };
                    let resp = client.get_object(get_obj_req).await;
                    println!("{:?}", key);
                    let mut result = resp.unwrap();
                    println!("{:?}", &result);

                    let stream = result.body.take().expect("no body");
                    println!("{:?}", &stream);
                    println!("{:?}", &output_file);
                    tokio::io::copy(&mut stream.into_async_read(), &mut output_file)
                        .await
                        .expect("copy fail");
                }
            }
        }
    } else if dest.starts_with("s3://") {
        println!("uploading");

        let parsed = Url::parse(dest).expect("invalid url");
        let bucket_name = parsed.host().unwrap().to_string();
        let raw_path = parsed.path();
        let path = raw_path.trim_start_matches("/").trim_end_matches("/");
        let local_dir = source;

        let dirname = Path::new(local_dir).file_name().unwrap().to_str().unwrap();
        let path_and_dirname = Path::new(path).join(dirname);
        let path_and_dirname = path_and_dirname.to_str().unwrap();

        let path_and_slash = [path, "/"].concat();
        let path_and_slash = path_and_slash.as_str();
        let remote_dir = if source.ends_with("/") {
            path_and_slash
        } else {
            path_and_dirname
        };

        let md = metadata(local_dir).unwrap();
        if md.is_file() {
            // println!("local_dir {:?}", local_dir);
            // println!("remove_path {:?}", path);
            println!("file");

            let mut file = File::open(&local_dir).unwrap();
            let mut buffer = vec![0; md.len() as usize];
            // todo : streaming upload
            file.read(&mut buffer).expect("buffer overflow");

            let put_request = PutObjectRequest {
                bucket: bucket_name.clone(),
                key: path.to_string(),
                body: Some(buffer.into()),
                ..Default::default()
            };

            client
                .put_object(put_request)
                .await
                .expect("Failed to put test object");

        } else {
            let files: Vec<_> =
                ScanDir::files()
                    .walk(local_dir, |iter| {
                        iter.map(|(ref entry, _)| {
                            entry.path().clone()
                        }).collect()
                    })
                    .unwrap();
            println!("files {:?}", files);

            for entry_path in files {
                let entry_remote_path =
                    entry_path.to_str().unwrap().trim_start_matches(local_dir);
                let entry_remote_path = [remote_dir, entry_remote_path].concat();
                let entry_remote_path = entry_remote_path.as_str();
                println!("{:?}", entry_remote_path);

                let md = fs::metadata(&entry_path).expect("unable to read metadata");
                let mut file = File::open(&entry_path).unwrap();
                let mut buffer = vec![0; md.len() as usize];
                file.read(&mut buffer).expect("buffer overflow");

                let put_request = PutObjectRequest {
                    bucket: bucket_name.clone(),
                    key: entry_remote_path.to_string(),
                    body: Some(buffer.into()),
                    ..Default::default()
                };

                // todo : fix files/ upload form
                client
                    .put_object(put_request)
                    .await
                    .expect("Failed to put object");
            };

        }
    } else {
        println!("both local address not supported");
    }

    Ok(())
}
#![allow(dead_code)]

extern crate clap;
use clap::{Arg, App, SubCommand};

extern crate s3;

use std::str;
use std::env;
use std::io;
use std::io::Read;
use std::io::ErrorKind;
use std::fs;
use std::fs::File;
use std::fs::metadata;
use std::path::Path;
use std::ffi::OsString;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::S3Error;

use url::{Url};
use scan_dir::ScanDir;

fn main() {
    let matches = 
        App::new("rusync")
            .version("0.5")
            .author("jiangplus")
            .about("s3 sync tools in rust")
            
            .subcommand(SubCommand::with_name("ls")
                .about("List bucket or objects")
                .arg(Arg::with_name("uri")
                .help("s3 bucket or object uri")))
            
            .subcommand(SubCommand::with_name("get")
                .about("Get file from bucket")
                .arg(Arg::with_name("source")
                .help("s3 object uri"))
                .arg(Arg::with_name("dest")
                .help("local path")))
            
            .subcommand(SubCommand::with_name("put")
                .about("Put file into bucket")
                .arg(Arg::with_name("source")
                .help("local file"))
                .arg(Arg::with_name("dest")
                .help("remove path")))
            
            .subcommand(SubCommand::with_name("rm")
                .about("Delete file from bucket")
                .arg(Arg::with_name("uri")
                .help("s3 object uri")))
            
            .subcommand(SubCommand::with_name("info")
                .about("Print information about Buckets or Files")
                .arg(Arg::with_name("uri")
                .help("s3 object uri")))
            
            .subcommand(SubCommand::with_name("sync")
                .about("Synchronize a directory tree to S3")
                .arg(Arg::with_name("source")
                .help("sync source"))
                .arg(Arg::with_name("dest")
                .help("sync destination")))
            
            .subcommand(SubCommand::with_name("msync")
                .about("Synchronize multiple directories to S3")
                .arg(Arg::with_name("source_dest")
                .multiple(true)
                .help("source and dest pair")))

            .get_matches();

    // println!("{:?}", matches.subcommand);

    match matches.subcommand() {
        ("ls",  Some(sub)) => {
          let uri = &sub.args["uri"].vals[0];
          println!("{:?}", uri);
          list_bucket(uri);
        },
        ("get",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
          println!("{:?}", source);
          get_object(source, dest);
        },
        ("put",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
          println!("{:?}", source);
          put_object(source, dest);
        },
        ("rm",  Some(sub)) => {
          let uri = &sub.args["uri"].vals[0];
          remove_object(uri);
        },
        ("info",  Some(sub)) => {
          let uri = &sub.args["uri"].vals[0];
          show_object(uri);
        },
        ("sync",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
          sync_dir(source, dest);
        },
        ("msync",  Some(sub)) => {
          let items = &sub.args["source_dest"].vals;
          for item in items {
            let pair:Vec<&str> = item.to_str().unwrap().split(":::").collect();
            // println!("{:?}", pair);
            // todo : pass string directly
            let source = OsString::from(pair[0]);
            let dest = OsString::from(pair[1]);
            sync_dir(&source, &dest);
          }

        },
        _             => {},
    }

}

fn list_bucket(uri: &OsString) -> Result<(), S3Error> {
    // println!("{:?}", uri);

    let region = Region::Custom {
        region: env::var("AWS_REGION").unwrap().into(),
        endpoint: env::var("AWS_HOST").unwrap().into(),
    };
    let credentials = Credentials::from_env_specific(
        Some("AWS_ACCESS_KEY_ID"),
        Some("AWS_SECRET_ACCESS_KEY"),
        None,
        None,
    )?;
    let uri = uri.to_str().unwrap();
    if !uri.starts_with("s3://") {
      panic!("{:?} is not s3 path", uri);
    }

    let parsed = Url::parse(uri)?;
    let bucket_name = parsed.host().unwrap().to_string();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").to_string();
    // println!("bucket {}", bucket_name);
    // println!("raw_path {}", raw_path);
    // println!("path {}", path);

    let bucket = Bucket::new(&bucket_name, region, credentials)?;

    let results = bucket.list_blocking(path, Some("/".to_string()))?;
    for (list, code) in results {
        assert_eq!(200, code);
        // println!("{:?}", list);
        if let Some(common_prefixes) = list.common_prefixes {
          for comm in common_prefixes {
            println!("dir s3://{}/{}", bucket_name, comm.prefix);
          }
        }

        for obj in list.contents {
            let e_tag = obj.e_tag.trim_start_matches("\"").trim_end_matches("\"");
            println!("{} {} {} s3://{}/{}", obj.last_modified, e_tag, obj.size, bucket_name, obj.key);
        }
    }
    Ok(())
}

fn get_object(source: &OsString, dest: &OsString) -> Result<(), S3Error> {
    let region = Region::Custom {
        region: env::var("AWS_REGION").unwrap().into(),
        endpoint: env::var("AWS_HOST").unwrap().into(),
    };
    let credentials = Credentials::from_env_specific(
        Some("AWS_ACCESS_KEY_ID"),
        Some("AWS_SECRET_ACCESS_KEY"),
        None,
        None,
    )?;
    let source = source.to_str().unwrap();
    if !source.starts_with("s3://") {
      panic!("{:?} is not s3 path", source);
    }

    let parsed = Url::parse(source)?;
    let bucket_name = parsed.host().unwrap().to_string();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").to_string();
    let dest = dest.to_str().unwrap();
    let local_file_path = dest;
    // println!("bucket {}", bucket_name);
    // println!("raw_path {}", raw_path);
    // println!("path {}", path);
    // println!("local_file_path {}", dest);

    let bucket = Bucket::new(&bucket_name, region, credentials)?;

    let local_file_dir = Path::new(&local_file_path).parent().unwrap();

    let mut output_file = match File::create(&local_file_path) {
        Ok(file) => file,
        Err(error) => match error.kind() {
            ErrorKind::NotFound => {
              fs::create_dir_all(&local_file_dir)?;
              File::create(&local_file_path).unwrap()
            },
            other_error => {
                panic!("Problem opening the file: {:?}", other_error)
            }
        },
    };
    let code = bucket.get_object_stream_blocking(&path, &mut output_file).unwrap();
    println!("Code: {}", code);
    Ok(())
}

fn put_object(source: &OsString, dest: &OsString) -> Result<(), S3Error> {
    let region = Region::Custom {
        region: env::var("AWS_REGION").unwrap().into(),
        endpoint: env::var("AWS_HOST").unwrap().into(),
    };
    let credentials = Credentials::from_env_specific(
        Some("AWS_ACCESS_KEY_ID"),
        Some("AWS_SECRET_ACCESS_KEY"),
        None,
        None,
    )?;
    let source = source.to_str().unwrap();
    let dest = dest.to_str().unwrap();
    if !dest.starts_with("s3://") {
      panic!("{:?} is not s3 path", dest);
    }

    let parsed = Url::parse(dest)?;
    let bucket_name = parsed.host().unwrap().to_string();
    let raw_path = parsed.path();
    let path = raw_path.trim_start_matches("/").trim_end_matches("/");
    let local_file_path = source;
    // println!("bucket {}", bucket_name);
    // println!("raw_path {}", raw_path);
    // println!("path {}", path);
    // println!("local_file_path {}", source);

    let bucket = Bucket::new(&bucket_name, region, credentials)?;

    let md = metadata(local_file_path).unwrap();
    if !md.is_file() {
        panic!("{:?} is not file", local_file_path);
    }

    let metadata = fs::metadata(&local_file_path).expect("unable to read file");
    let mut buffer = vec![0; metadata.len() as usize];
    let mut file = File::open(&local_file_path).unwrap();
    file.read(&mut buffer).expect("buffer overflow");
    
    // todo : streaming upload
    let status_code = bucket.put_object_blocking(path, &buffer, "application/octet-stream").unwrap();
    println!("result: {}", status_code.1);

    Ok(())
}

fn remove_object(uri: &OsString) -> Result<(), S3Error> {
    // let region_name = env::var("AWS_REGION").unwrap();
    // let endpoint = env::var("AWS_HOST").unwrap();
    
    println!("{:?}", uri);
    Ok(())
}

fn show_object(uri: &OsString) -> Result<(), S3Error> {
    // let region_name = env::var("AWS_REGION").unwrap();
    // let endpoint = env::var("AWS_HOST").unwrap();
    
    println!("{:?}", uri);
    Ok(())
}

fn sync_dir(source: &OsString, dest: &OsString) -> Result<(), S3Error> {
    let region = Region::Custom {
        region: env::var("AWS_REGION").unwrap().into(),
        endpoint: env::var("AWS_HOST").unwrap().into(),
    };
    let credentials = Credentials::from_env_specific(
        Some("AWS_ACCESS_KEY_ID"),
        Some("AWS_SECRET_ACCESS_KEY"),
        None,
        None,
    )?;

    let source = source.to_str().unwrap();
    let dest = dest.to_str().unwrap();

    if source.starts_with("s3://") && dest.starts_with("s3://") {
      println!("both s3 address not supported");
    } else if source.starts_with("s3://") {
      println!("downsync");

      let parsed = Url::parse(source)?;
      let bucket_name = parsed.host().unwrap().to_string();
      let raw_path = parsed.path();
      let path = raw_path.trim_start_matches("/");
      let local_dir = dest;
      // println!("bucket {}", bucket_name);
      // println!("raw_path {}", raw_path);
      // println!("path {}", path);
      // println!("local_dir {}", dest);

      let bucket = Bucket::new(&bucket_name, region, credentials)?;

      let results = bucket.list_blocking(path.to_string(), None)?;
      for (list, code) in results {
          assert_eq!(200, code);

          for obj in list.contents {
              // println!("\n");
              let e_tag = obj.e_tag.trim_start_matches("\"").trim_end_matches("\"");
              
              // println!("{:?}", obj);
              // println!("etag {}", e_tag);
              let sub_path = obj.key.trim_start_matches(path);
              // println!("key {}", obj.key);
              let local_file_path = 
                if raw_path.ends_with("/") {
                    Path::new(local_dir).join(sub_path)
                } else {
                    let mid_path = Path::new(path).file_name().unwrap().to_str().unwrap();
                    Path::new(local_dir).join([mid_path, sub_path].concat())
                };
              let local_file_dir = Path::new(&local_file_path).parent().unwrap();
              // println!("local_file_path {:?}", local_file_path);
              // println!("local_file_dir {:?}", local_file_dir);

              if Path::new(&local_file_path).exists() && checkfile(&local_file_path, e_tag) {
                println!("skip {:?} -> {:?}", obj.key, local_file_path);
              } else {
                let mut output_file = match File::create(&local_file_path) {
                    Ok(file) => file,
                    Err(error) => match error.kind() {
                        ErrorKind::NotFound => {
                          fs::create_dir_all(&local_file_dir)?;
                          File::create(&local_file_path).unwrap()
                        },
                        other_error => {
                            panic!("Problem opening the file: {:?}", other_error)
                        }
                    },
                };

                let code = bucket.get_object_stream_blocking(&obj.key, &mut output_file).unwrap();
                println!("Code: {}", code);
              }
          }
      }

    } else if dest.starts_with("s3://") {
      println!("upsync");

      let parsed = Url::parse(dest)?;
      let bucket_name = parsed.host().unwrap().to_string();
      let raw_path = parsed.path();
      let path = raw_path.trim_start_matches("/").trim_end_matches("/");
      let local_dir = source;
      // println!("bucket {}", bucket_name);
      // println!("raw_path {}", raw_path);
      // println!("path {}", path);
      // println!("local_dir {}", source);

      let bucket = Bucket::new(&bucket_name, region, credentials)?;

      // todo : check whether local file hash match remote file
      // for obj in &list.contents {
      //     println!("\n");
      //     let sub_path = obj.key.trim_start_matches(path);
      //     println!("key {}", obj.key);
      // }

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
      // println!("remote_dir {}", remote_dir);

      let md = metadata(local_dir).unwrap();
      if md.is_file() {
        // println!("local_dir {:?}", local_dir);
        // println!("remove_path {:?}", path);
        // todo : single file upload
        println!("file");

        let metadata = fs::metadata(&local_dir).expect("unable to read file");
        let mut buffer = vec![0; metadata.len() as usize];
        let mut file = File::open(&local_dir).unwrap();
        file.read(&mut buffer).expect("buffer overflow");
        
        // todo : streaming upload
        let status_code = bucket.put_object_blocking(path, &buffer, "application/octet-stream").unwrap();
        println!("result: {}", status_code.1);

      } else {
        let _all_files: Vec<_> = ScanDir::files().walk(local_dir, |iter| {
            iter.map(|(ref entry, _)| {
              let entry_path = entry.path();
              // println!("entry_path {:?}", entry_path);
              let entry_remote_path = entry_path.to_str().unwrap().trim_start_matches(local_dir);
              let entry_remote_path = [remote_dir, entry_remote_path].concat();
              // println!("entry_remote_path2 {:?}", entry_remote_path);
              let entry_remote_path = entry_remote_path.as_str();
              // println!("upload {:?} -> {:?}", entry_path, entry_remote_path);
              // println!("entry_remote_path {:?}, entry_path {:?}", &entry_remote_path, &entry_path);

              let metadata = fs::metadata(&entry_path).expect("unable to read metadata");
              let mut buffer = vec![0; metadata.len() as usize];
              let mut file = File::open(&entry_path).unwrap();
              file.read(&mut buffer).expect("buffer overflow");
              

              // todo : streaming upload
              let status_code = bucket.put_object_blocking(entry_remote_path, &buffer, "application/octet-stream").unwrap();
              println!("result: {}", status_code.1);

              entry.path()
            }).collect()
        }).unwrap();
      }

    } else {
      println!("both local address not supported");
    }

    Ok(())
}

fn sync_dirs(source_dest: &OsString) -> Result<(), S3Error> {
    println!("{:?}", source_dest);
    Ok(())
}

pub fn checkfile(file_name: &Path, expected_hash: &str) -> bool {
    let mut file = File::open(file_name).unwrap();
    let mut md5_context = md5::Context::new();
    io::copy(&mut file, &mut md5_context).unwrap();
    let hash = md5_context.compute();
    let hash = base16::encode_lower(&hash.0);
    return hash == expected_hash
}



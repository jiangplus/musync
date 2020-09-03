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
          let source_dest = &sub.args["source_dest"].vals[0];
          sync_dirs(source_dest);
        },
        _             => {},
    }

}

fn list_bucket(uri: &OsString) -> Result<(), S3Error> {
    println!("{:?}", uri);

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
    println!("bucket {}", bucket_name);
    println!("raw_path {}", raw_path);
    println!("path {}", path);

    let bucket = Bucket::new(&bucket_name, region, credentials)?;

    let results = bucket.list_blocking(path, Some("/".to_string()))?;
    for (list, code) in results {
        assert_eq!(200, code);
        println!("{:?}", list);
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
    println!("bucket {}", bucket_name);
    println!("raw_path {}", raw_path);
    println!("path {}", path);
    println!("local_file_path {}", dest);

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
    println!("{:?}", source);
    Ok(())
}

fn put_object(source: &OsString, dest: &OsString) -> Result<(), S3Error> {
    // let region_name = env::var("AWS_REGION").unwrap();
    // let endpoint = env::var("AWS_HOST").unwrap();
    
  println!("{:?}", source);
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
    // let region_name = env::var("AWS_REGION").unwrap();
    // let endpoint = env::var("AWS_HOST").unwrap();
    
  println!("{:?}", source);
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



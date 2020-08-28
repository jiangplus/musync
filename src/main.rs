extern crate s3;

use std::str;
use std::env;
use std::path::Path;
use std::fs::File;
use std::io::ErrorKind;
use std::fs;
use std::io;
use std::io::Read;
use std::fs::metadata;

use url::{Url};
use scan_dir::ScanDir;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::S3Error;

struct Storage {
    region: Region,
    credentials: Credentials,
}

fn typeid<T: std::any::Any>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

pub fn checkfile(file_name: &Path, expected_hash: &str) -> bool {
  let mut file = File::open(file_name).unwrap();
  let mut md5_context = md5::Context::new();
  io::copy(&mut file, &mut md5_context).unwrap();
  let hash = md5_context.compute();
  let hash = base16::encode_lower(&hash.0);
  return hash == expected_hash
}

pub fn main() -> Result<(), S3Error> {
    let region_name = env::var("AWS_REGION").unwrap();
    let endpoint = env::var("AWS_HOST").unwrap();

    let args: Vec<String> = env::args().collect();
    let source = &args[2];
    let dest = &args[3];

    let aws = Storage {
        region: Region::Custom {
            region: region_name.into(),
            endpoint: endpoint.into(),
        },
        credentials: Credentials::from_env_specific(
            Some("AWS_ACCESS_KEY_ID"),
            Some("AWS_SECRET_ACCESS_KEY"),
            None,
            None,
        )?
    };

    if source.starts_with("s3://") && dest.starts_with("s3://") {
      println!("both s3 address not supported");
    } else if source.starts_with("s3://") {
      println!("downsync");

      let parsed = Url::parse(source)?;
      let bucket_name = parsed.host().unwrap().to_string();
      let raw_path = parsed.path();
      let path = raw_path.trim_start_matches("/");
      let local_dir = dest;
      println!("bucket {}", bucket_name);
      println!("raw_path {}", raw_path);
      println!("path {}", path);
      println!("local_dir {}", dest);

      let bucket = Bucket::new(&bucket_name, aws.region, aws.credentials)?;

      let results = bucket.list_blocking(path.to_string(), None)?;
      for (list, code) in results {
          assert_eq!(200, code);

          for obj in list.contents {
              println!("\n");
              let e_tag = obj.e_tag.trim_start_matches("\"").trim_end_matches("\"");
              
              println!("{:?}", obj);
              println!("etag {}", e_tag);
              let sub_path = obj.key.trim_start_matches(path);
              println!("key {}", obj.key);
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
      println!("bucket {}", bucket_name);
      println!("raw_path {}", raw_path);
      println!("path {}", path);
      println!("local_dir {}", source);

      let bucket = Bucket::new(&bucket_name, aws.region, aws.credentials)?;

      let results = bucket.list_blocking(path.to_string(), None)?;
      for (list, code) in results {
          assert_eq!(200, code);
          
          // todo : check whether local file hash match remote file
          // for obj in &list.contents {
          //     println!("\n");
          //     let sub_path = obj.key.trim_start_matches(path);
          //     println!("key {}", obj.key);
          // }

          let dirname = Path::new(local_dir).file_name().unwrap().to_str().unwrap();
          let path_and_dirname = Path::new(path).join(dirname);
          let path_and_dirname = path_and_dirname.to_str().unwrap();

          let remote_dir = if source.ends_with("/") {
            path
          } else {
            path_and_dirname
          };
          println!("remote_dir {}", remote_dir);

          let md = metadata(local_dir).unwrap();
          if md.is_file() {
            // todo : single file upload
            println!("file");
          } else {
            let _all_files: Vec<_> = ScanDir::files().walk(local_dir, |iter| {
                iter.map(|(ref entry, _)| {
                  let entry_path = entry.path();
                  let entry_remote_path = entry_path.to_str().unwrap().trim_start_matches(local_dir);
                  let remote_dir_path = Path::new(remote_dir);
                  let entry_remote_path = remote_dir_path.join(entry_remote_path);
                  let entry_remote_path = entry_remote_path.to_str().unwrap();
                  println!("upload {:?} -> {:?}", entry_path, entry_remote_path);

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
      }

    } else {
      println!("both local address not supported");
    }

    Ok(())
}

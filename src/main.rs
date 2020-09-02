extern crate clap;
use clap::{Arg, App, SubCommand};

fn main() {
    let matches = 
        App::new("rusync")
            .version("0.5")
            .author("jiangplus <jiang.plus.times@gmail.com>")
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
        },
        ("get",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
          println!("{:?}", source);
        },
        ("put",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
          println!("{:?}", source);
        },
        ("rm",  Some(sub)) => {
          let source = &sub.args["uri"].vals[0];
        },
        ("info",  Some(sub)) => {
          let source = &sub.args["uri"].vals[0];
        },
        ("sync",  Some(sub)) => {
          let source = &sub.args["source"].vals[0];
          let dest = &sub.args["dest"].vals[0];
        },
        ("msync",  Some(sub)) => {
          let source_dest = &sub.args["source_dest"].vals[0];
        },
        _             => {},
    }

}
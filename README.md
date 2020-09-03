### rusync

simple s3 sync tool in rust

### Set environment variables

  export AWS_ACCESS_KEY_ID

  export AWS_SECRET_ACCESS_KEY

  export AWS_REGION

  export AWS_HOST # like : https://s3.cn-northwest-1.amazonaws.com.cn

### commands

  rusync ls s3://bucket/path

  rusync get s3://bucket/path local_path
  
  rusync put local_path s3://bucket/path

  rusync sync local_data s3://bucket/path
  
  rusync sync s3://bucket/path local_data

  rusync msync s3://bucket/path:::local_data s3://bucket/path:::local_data

`msync` is the command to sync multiple destinations at once, which acts the same as calling `sync` in multiple times, each pair is concatenate by `:::`

### Todo

Currently object streaming upload is not working, data has to be read into buffer.

See `https://github.com/durch/rust-s3/issues/106`.

Use `https://github.com/rusoto/rusoto` as aws sdk in the future

### License

MIT License

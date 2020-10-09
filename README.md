### musync

simple s3 sync tool in rust

### Set environment variables

  export AWS_ACCESS_KEY_ID

  export AWS_SECRET_ACCESS_KEY

  export AWS_REGION

  export AWS_HOST # like : https://s3.cn-northwest-1.amazonaws.com.cn

### commands

  musync ls s3://bucket/path

  musync get s3://bucket/path local_path
  
  musync put local_path s3://bucket/path

  musync sync local_data s3://bucket/path
  
  musync sync s3://bucket/path local_data

  musync msync s3://bucket/path:::local_data s3://bucket/path:::local_data

`msync` is the command to sync multiple destinations at once, which acts the same as calling `sync` multiple times, each pair is concatenate by `:::`

### License

MIT License

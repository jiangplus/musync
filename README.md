simple s3 sync tool in rust

set environment variables:

  export AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY
  export AWS_REGION
  export AWS_HOST

commands:

  rusync local_data s3://bucket/path
  rusync s3://bucket/path local_data

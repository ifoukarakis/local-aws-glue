# local-aws-glue

AWS Glue offers a really nice set of tools. However, in order to get started either an AWS account is required or by [using a docker image plus some setup](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/). 

This repo offers an example `docker-compose.yml` file, accompanied by a project setup. You can use this setup to jump start your Glue experimentation.

## Features

- Run Glue locally either via jobs or via Jupyter Lab
- Local S3 using [Localstack](https://localstack.cloud/)

## Requirements

Docker and docker compose (or similar) is all you need.

## Configuration

S3 setup can optionally be done on container startup. Just edit [.aws/buckets.sh](.aws/buckets.sh). This bash script can contain any set of AWS CLI S3 operations.


## Running

Simply run

```bash
docker compose up
```

## Jupyter notebooks

JupyterLab is available at [http://127.0.0.1:8888/](http://127.0.0.1:8888/). Any notebooks under [notebooks/](notebooks/) will be available. A couple of sample notebooks exist to get you started.

## S3 operations using AWS CLI

AWS CLI can be used for managing buckets and objects. The only requirement is that mock  credentials have been defined. Here's an example:

```bash
AWS_ACCESS_KEY_ID=mock AWS_SECRET_ACCESS_KEY=mock aws --endpoint-url=http://localhost:4566  s3 ls
```

## Running test jobs

All jobs under [jobs/](jobs/) will be copied automatically under `/opt/jobs` inside Glue docker container. 

Connect to the docker container for glue. The command should be similar to:

```bash
docker exec -it local-aws-glue-glue-1 /bin/bash
```

Then using the container's bash shell use `glue-spark-submit` to run a job. For example, you can run `orders.py` by running:

```bash
glue-spark-submit --master local\[*\] /opt/jobs/orders.py
````
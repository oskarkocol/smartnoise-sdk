terraform {
  required_version = ">= 0.13.1"
  backend "local" {}
  required_providers {
  aws = {
      source  = "hashicorp/aws"
      version = "~> 4.19"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "random_password" "redshift_password" {
  length           = 16
  special          = false
}

resource "random_string" "redshift_user" {
  length           = 5
  special          = false
  numeric          = false
  lower            = true
  upper            = false
}

resource "aws_s3_bucket" "smartnoise_ci_bucket" {
  bucket_prefix = "smartnoise-ci-bucket-"
  force_destroy = true

  tags = {
    Name        = "CI bucket"
    Environment = "CI"
  }
}

output "user" {
  value = random_string.redshift_user.result
}

output "pass" {
  value = random_password.redshift_password.result
}

resource "aws_redshift_cluster" "smartnoise_ci_warehouse" {
  cluster_identifier = "smartnoise-ci-warehouse"
  database_name      = "pums"
  master_username    = "redshift_admin" #random_string.redshift_user.result
  master_password    = "#HasloRedshift1234" #random_password.redshift_password.result
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  #TODO: attach Redshift Role
  #default_iam_role_arn = "arn:aws:iam::702586998089:role/redshift-s3-access"
  #iam_roles = ["arn:aws:iam::702586998089:role/redshift-s3-access"]
  automated_snapshot_retention_period = 0
  skip_final_snapshot = true
}

# load PUMS
resource "aws_s3_object" "pums" {
  bucket = aws_s3_bucket.smartnoise_ci_bucket.id
  key    = "PUMS.csv"
  source = "../../../../datasets/PUMS.csv"
  etag = filemd5("../../../../datasets/PUMS.csv")
}

resource "aws_redshiftdata_statement" "pums_load" {
  cluster_identifier = aws_redshift_cluster.smartnoise_ci_warehouse.cluster_identifier
  database           = aws_redshift_cluster.smartnoise_ci_warehouse.database_name
  db_user            = aws_redshift_cluster.smartnoise_ci_warehouse.master_username
  sql                = <<EOF
  select 1;
  create table if not exists pums (
    age integer,
    sex varchar(100),
    educ integer,
    race varchar(100),
    income numeric(10, 2),
    married varchar(100)
  );
  copy pums from 's3://${aws_s3_bucket.smartnoise_ci_bucket.id}/${aws_s3_object.pums.id}'
  credentials 'aws_access_key_id=${var.aws_access_key_id};aws_secret_access_key=${var.aws_secret_access_key}'
  csv
  ignoreheader 1;
  EOF
}

# load PUMS_dup
resource "aws_s3_object" "pums_dup" {
  bucket = aws_s3_bucket.smartnoise_ci_bucket.id
  key    = "PUMS_dup.csv"
  source = "../../../../datasets/PUMS_dup.csv"
  etag = filemd5("../../../../datasets/PUMS_dup.csv")
}

resource "aws_redshiftdata_statement" "pums_dup_load" {
  cluster_identifier = aws_redshift_cluster.smartnoise_ci_warehouse.cluster_identifier
  database           = aws_redshift_cluster.smartnoise_ci_warehouse.database_name
  db_user            = aws_redshift_cluster.smartnoise_ci_warehouse.master_username
  sql                = <<EOF
  select 1;
  create table if not exists pums_dup (
    age integer,
    sex varchar(100),
    educ varchar(100),
    race varchar(100),
    income numeric(10, 2),
    married varchar(100),
    pid integer
  );
  copy pums_dup from 's3://${aws_s3_bucket.smartnoise_ci_bucket.id}/${aws_s3_object.pums_dup.id}'
  credentials 'aws_access_key_id=${var.aws_access_key_id};aws_secret_access_key=${var.aws_secret_access_key}'
  csv
  ignoreheader 1;
  EOF
}

# load PUMS_large
resource "aws_s3_object" "pums_large" {
  bucket = aws_s3_bucket.smartnoise_ci_bucket.id
  key    = "PUMS_large.csv"
  source = "../../../../datasets/PUMS_large.csv"
  etag = filemd5("../../../../datasets/PUMS_large.csv")
}

resource "aws_redshiftdata_statement" "pums_large_load" {
  cluster_identifier = aws_redshift_cluster.smartnoise_ci_warehouse.cluster_identifier
  database           = aws_redshift_cluster.smartnoise_ci_warehouse.database_name
  db_user            = aws_redshift_cluster.smartnoise_ci_warehouse.master_username
  sql                = <<EOF
  select 1;
  create table if not exists pums_large (
    personid integer,
    state integer,
    puma varchar(100),
    sex varchar(100),
    age integer,
    educ integer,
    income numeric(10, 2),
    latino boolean,
    black boolean,
    asian boolean,
    married boolean
  );
  copy pums_large from 's3://${aws_s3_bucket.smartnoise_ci_bucket.id}/${aws_s3_object.pums_large.id}'
  credentials 'aws_access_key_id=${var.aws_access_key_id};aws_secret_access_key=${var.aws_secret_access_key}'
  csv
  ignoreheader 1;
  EOF
}

# load PUMS_null
resource "aws_s3_object" "pums_null" {
  bucket = aws_s3_bucket.smartnoise_ci_bucket.id
  key    = "PUMS_null.csv"
  source = "../../../../datasets/PUMS_null.csv"
  etag = filemd5("../../../../datasets/PUMS_null.csv")
}

resource "aws_redshiftdata_statement" "pums_null_load" {
  cluster_identifier = aws_redshift_cluster.smartnoise_ci_warehouse.cluster_identifier
  database           = aws_redshift_cluster.smartnoise_ci_warehouse.database_name
  db_user            = aws_redshift_cluster.smartnoise_ci_warehouse.master_username
  sql                = <<EOF
  select 1;
  create table if not exists pums_null (
    age integer,
    sex varchar(100),
    educ integer,
    race varchar(100),
    income numeric(10, 2),
    married varchar(100),
    pid integer
  );
  copy pums_null from 's3://${aws_s3_bucket.smartnoise_ci_bucket.id}/${aws_s3_object.pums_null.id}'
  credentials 'aws_access_key_id=${var.aws_access_key_id};aws_secret_access_key=${var.aws_secret_access_key}'
  csv
  ignoreheader 1;
  EOF
}

# load PUMS_pid
resource "aws_s3_object" "pums_pid" {
  bucket = aws_s3_bucket.smartnoise_ci_bucket.id
  key    = "PUMS_pid.csv"
  source = "../../../../datasets/PUMS_pid.csv"
  etag = filemd5("../../../../datasets/PUMS_pid.csv")
}

resource "aws_redshiftdata_statement" "pums_pid_load" {
  cluster_identifier = aws_redshift_cluster.smartnoise_ci_warehouse.cluster_identifier
  database           = aws_redshift_cluster.smartnoise_ci_warehouse.database_name
  db_user            = aws_redshift_cluster.smartnoise_ci_warehouse.master_username
  sql                = <<EOF
  create table if not exists pums_pid (
    age integer,
    sex varchar(100),
    educ varchar(100),
    race varchar(100),
    income numeric(10, 2),
    married varchar(100),
    pid integer
  );
  copy pums_pid from 's3://${aws_s3_bucket.smartnoise_ci_bucket.id}/${aws_s3_object.pums_pid.id}'
  credentials 'aws_access_key_id=${var.aws_access_key_id};aws_secret_access_key=${var.aws_secret_access_key}'
  csv
  ignoreheader 1;
  EOF
}

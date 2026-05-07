from __future__ import annotations

from pyspark.sql import SparkSession

S3_BUCKET = "binance-iceberg-lake"
AWS_REGION = "ap-northeast-2"
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/warehouse/"


def get_spark(app_name: str) -> SparkSession:
    """Iceberg + Glue catalog 가 설정된 SparkSession.

    processed/serving job에서 사용한다.
    spark-submit 시 --packages:
        org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0
        org.apache.iceberg:iceberg-aws-bundle:1.7.0
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue.warehouse", WAREHOUSE_PATH)
        .config(
            "spark.sql.catalog.glue.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.glue.glue.region", AWS_REGION)
        .config("spark.sql.catalog.glue.s3.region", AWS_REGION)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .getOrCreate()
    )


def get_spark_streaming(app_name: str) -> SparkSession:
    """Kafka → S3 Parquet streaming용 SparkSession.

    Iceberg 설정 없음. raw zone은 plain Parquet이기 때문이다.
    spark-submit 시 --packages:
        org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
        org.apache.hadoop:hadoop-aws:3.3.4
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .getOrCreate()
    )
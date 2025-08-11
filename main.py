from ice_optimizer.core import Iceoptimizer
from pyspark.sql import SparkSession


AWS_ENDPOINT    = "your_aws_endpoint"
AWS_ACCESS_KEY  = "key"
AWS_SECRET_KEY  = "secret"

spark = (
        SparkSession.builder
        .appName("ice optimizer test")
        .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4") \
        .getOrCreate()
    )


iceoptimizer = Iceoptimizer(max_concurrent_tasks=10, spark=spark, root_folder="./example")
rr = iceoptimizer.get_generated_sqls()
spark.stop()
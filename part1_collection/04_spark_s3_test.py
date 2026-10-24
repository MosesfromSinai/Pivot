from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("CS179G_S3_Test")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Use anonymous access for the public S3 file
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )

    print("Loading professor's S3 dataset...")

    sc = spark.sparkContext
    jvm = sc._jvm

    # Access the S3 object directly
    path = jvm.org.apache.hadoop.fs.Path(
        "s3a://vagelis-testbucket1/test.csv"
    )

    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    stream = fs.open(path)

    try:
        raw_bytes = stream.readAllBytes()
        content = bytes(
            (int(b) & 0xFF) for b in raw_bytes
        ).decode("utf-8")
    finally:
        stream.close()

    # Convert the S3 contents into a Spark DataFrame
    lines = sc.parallelize(content.splitlines(), 1)

    df = spark.read.csv(
        lines,
        header=True,
        inferSchema=True
    )

    print("Dataset columns:")
    df.printSchema()

    print("Example records:")
    df.show(5, truncate=False)

    # Count people in ZIP code 78727
    count = df.filter(
        F.col("zipcode").cast("string") == "78727"
    ).count()

    print(f"People in ZIP code 78727: {count}")

finally:
    spark.stop()

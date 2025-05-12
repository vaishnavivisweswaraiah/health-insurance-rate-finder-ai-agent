
import os
# Ensure JAVA_HOME is set so Spark can initialize correctly in a fresh process.
os.environ["JAVA_HOME"] = "/Users/vaishnavi/Desktop/Research/LLM/health-insurance-bot/sparkJava/jdk-11.0.26+4/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
from pyspark.sql import SparkSession
#Initialize Spark Session
spark = SparkSession.builder.appName("Health Insurance").getOrCreate()
def read_data_spark(file_path: str, file_format: str, **options):
    return spark.read.format(file_format).options(**options).load(file_path)

def write_data_spark(file_path: str, file_format: str, df, mode: str = "overwrite", partition_by: list[str] | None = None, **options):
    df.write.format(file_format) \
        .mode(mode) \
        .partitionBy(*(partition_by or [])) \
        .options(**options) \
        .save(file_path)
    


def read_last_processed_offset(OFFSET_TRACK_FILE: str) -> int:
    if os.path.exists(OFFSET_TRACK_FILE):
        with open(OFFSET_TRACK_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def write_last_processed_offset(offset: int,OFFSET_TRACK_FILE: str):
    with open(OFFSET_TRACK_FILE, "w") as f:
        f.write(str(offset))



import sys
import json
from dependencies.spark import get_spark_session
from dependencies.logging import get_logger
import os
os.environ["HADOOP_OPTS"] = "-Djava.library.path="

def main(config_path):
    logger = get_logger("ETLJob")
    spark = get_spark_session("PySparkETLExample")

    logger.info(f"Reading config from {config_path}")
    with open(config_path, 'r') as f:
        config = json.load(f)

    df = spark.read.csv(config["source_path"], header=config["header"], sep=config["delimiter"])
    logger.info("Data loaded successfully")
    df.show()

    df.coalesce(1)
      .write
    .mode("overwrite")
  .option("header", True)
  .option("codec", "none")
  .option("path", config["destination_path"])
  .format("csv")
  .save()
    logger.info("Data written successfully")

if __name__ == "__main__":
    main(sys.argv[1])

from pathlib import Path

from Migration.logs.logs import logger
from pyspark.sql import SparkSession

logger.info("Starting Spark session...")

# Usa o driver que acompanha o projeto. No Windows, spark.jars.packages tenta
# baixar/copiar o arquivo via Hadoop e exige winutils.exe/HADOOP_HOME.
postgres_driver = (
    Path(__file__).resolve().parents[1]
    / "drivers"
    / "postgresql-42.7.12.jar"
)

if not postgres_driver.is_file():
    raise FileNotFoundError(f"Driver PostgreSQL nao encontrado: {postgres_driver}")

driver_path = str(postgres_driver)

app_spark = (
    SparkSession.builder
    .appName("Migration")
    .master("local[*]")
    .config("spark.driver.extraClassPath", driver_path)
    .config("spark.executor.extraClassPath", driver_path)
    .getOrCreate()
)
logger.info("Spark session started.")


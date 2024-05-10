import pytest
from pyspark.sql import SparkSession
import sys

sys.path.insert(0, "./src")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local-cluster[2,2,2048]")
        .config("spark.sql.executorMemory", "2g")
        .appName("Document validator testing")
        .getOrCreate()
    )

    return spark

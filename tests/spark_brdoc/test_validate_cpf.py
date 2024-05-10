import pytest
from pyspark.sql import SparkSession
from spark_brdoc import validateCPF


class TestCPFValidation:
    @pytest.mark.parametrize(
        "cpf",
        ["67047129090", "55586223007", "79051484089", "70265163021", "76973663044"],
    )
    def test_valid_cpf(self, spark: SparkSession, cpf: str):
        testDf = spark.createDataFrame([(cpf,)], schema="cpf string").withColumn(
            "is_valid", validateCPF("cpf")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 1
        assert testDf.filter(testDf["is_valid"] == False).count() == 0

    @pytest.mark.parametrize("cpf", ["67047129091", "55586223027"])
    def test_invalid_cpf(self, spark: SparkSession, cpf: str):
        testDf = spark.createDataFrame([(cpf,)], schema="cpf string").withColumn(
            "is_valid", validateCPF("cpf")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 0
        assert testDf.filter(testDf["is_valid"] == False).count() == 1

    @pytest.mark.parametrize(
        "cpf",
        [
            "00000000000",  # All digits equal = invalid CPF
            "1234567",  # Less than 11 digits
            "12345678901234",  # More than 11 digits
            "6704712a090",  # almost valid doc, except by the "a" char.
            "",  # less than 11 chars
        ],
    )
    def test_invalid_cpf_format(self, spark: SparkSession, cpf: str):
        testDf = spark.createDataFrame([(cpf,)], schema="cpf string").withColumn(
            "is_valid", validateCPF("cpf")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 0
        assert testDf.filter(testDf["is_valid"] == False).count() == 1

    def test_mix_valid_invalid_invalid_format_cpfs(self, spark: SparkSession):
        # 1 valid, the rest invalid.
        testDf = spark.createDataFrame(
            [
                ("67047129090",),
                ("67047129091",),
                ("00000000000",),
                ("33333333333",),
                ("1234567",),
                ("12345678901234",),
                ("6704712a090",),
                ("",),
            ],
            schema="cpf string",
        ).withColumn("is_valid", validateCPF("cpf"))

        assert testDf.filter(testDf["is_valid"] == True).count() == 1
        assert testDf.filter(testDf["is_valid"] == False).count() == 7

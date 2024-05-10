import pytest
from pyspark.sql import SparkSession
from spark_brdoc import validateCNPJ


class TestCPFValidation:
    @pytest.mark.parametrize(
        "cnpj",
        [
            "10942381000181",
            "66784684000178",
            "33391611000167",
            "64116773000110",
            "36143513000107",
        ],
    )
    def test_valid_cnpj(self, spark: SparkSession, cnpj: str):
        testDf = spark.createDataFrame([(cnpj,)], schema="cnpj string").withColumn(
            "is_valid", validateCNPJ("cnpj")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 1
        assert testDf.filter(testDf["is_valid"] == False).count() == 0

    @pytest.mark.parametrize("cnpj", ["10942381000182", "36143513000117"])
    def test_invalid_cnpj(self, spark: SparkSession, cnpj: str):
        testDf = spark.createDataFrame([(cnpj,)], schema="cnpj string").withColumn(
            "is_valid", validateCNPJ("cnpj")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 0
        assert testDf.filter(testDf["is_valid"] == False).count() == 1

    @pytest.mark.parametrize(
        "cnpj",
        [
            "00000000000000",  # All digits equal = invalid CPF
            "1234567",  # Less than 14 digits
            "123456789012345123",  # More than 14 digits
            "333916110a0167",  # almost valid doc, except by the "a" char.
            "",  # less than 14 chars
        ],
    )
    def test_invalid_cnpj_format(self, spark: SparkSession, cnpj: str):
        testDf = spark.createDataFrame([(cnpj,)], schema="cnpj string").withColumn(
            "is_valid", validateCNPJ("cnpj")
        )
        assert testDf.filter(testDf["is_valid"] == True).count() == 0
        assert testDf.filter(testDf["is_valid"] == False).count() == 1

    def test_mix_valid_invalid_invalid_format_cnpj(self, spark: SparkSession):
        # 1 valid, the rest invalid.
        testDf = spark.createDataFrame(
            [
                ("10942381000181",),
                ("00000000000000",),
                ("1234567",),
                ("123456789012345123",),
                ("333916110a0167",),
                ("",),
            ],
            schema="cnpj string",
        ).withColumn("is_valid", validateCNPJ("cnpj"))

        assert testDf.filter(testDf["is_valid"] == True).count() == 1
        assert testDf.filter(testDf["is_valid"] == False).count() == 5

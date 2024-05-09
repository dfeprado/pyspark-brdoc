from pyspark.sql import DataFrame, functions as sqlfn, Window, Column


class CPFValidator:
    _KNOWN_INVALID_DOCS = [
        "00000000000",
        "11111111111",
        "22222222222",
        "33333333333",
        "44444444444",
        "55555555555",
        "66666666666",
        "77777777777",
        "88888888888",
        "99999999999",
    ]

    class __Col:
        BASE_DIGIT = "_base"
        VERIFICATION_DIGITS = "_vdigs"
        DIG_1_WEIGTH = "_w1"
        DIG_2_WEIGTH = "_w2"
        PRODUCT_DIG_1 = "_dw1"
        PRODUCT_DIG_2 = "_dw2"
        SUM_PRODUCT_DIG_1 = "_sd1"
        SUM_PRODUCT_DIG_2 = "_sd2"
        COMPUTED_DIG_1 = "_d1"
        COMPUTED_DIG_2 = "_d2"
        COMPUTED_VERIFICATION_DIGITS = "_computed_vdigs"

    def __init__(self, docColName: str, outputColName: str):
        self.__docColName = docColName
        self.__outputColName = outputColName

    def validate(self, df: DataFrame) -> DataFrame:
        invalidRowsDf = df.filter(
            (df[self.__docColName].isin(CPFValidator._KNOWN_INVALID_DOCS))
            | (~df[self.__docColName].rlike(r"^[0-9]{11}$"))
        )

        workingDf = df.exceptAll(invalidRowsDf)

        # Quebra o CPF em 3 partes:
        #   1. CPF completo
        #   2. Dígitos de base (1-8), em um array
        #   3. Dígitos verificadores.
        df2 = workingDf.select(
            self.__docColName,
            sqlfn.split(df[self.__docColName].substr(1, 9), "", 9).alias(
                CPFValidator.__Col.BASE_DIGIT
            ),
            df[self.__docColName]
            .substr(10, 12)
            .alias(CPFValidator.__Col.VERIFICATION_DIGITS),
        )

        # Explode os dígitos de base para cada um na sua linha
        df3 = df2.select(
            self.__docColName,
            sqlfn.explode(CPFValidator.__Col.BASE_DIGIT).alias(
                CPFValidator.__Col.BASE_DIGIT
            ),
            CPFValidator.__Col.VERIFICATION_DIGITS,
        ).withColumn(
            CPFValidator.__Col.BASE_DIGIT,
            sqlfn.col(CPFValidator.__Col.BASE_DIGIT).cast("int"),
        )

        # Cria as colunas de peso para cálculo dos dígitos 1 e 2
        df5 = df3.withColumn(
            CPFValidator.__Col.DIG_1_WEIGTH,
            sqlfn.row_number().over(
                Window().partitionBy(self.__docColName).orderBy(self.__docColName)
            ),
        ).withColumn(
            CPFValidator.__Col.DIG_2_WEIGTH,
            sqlfn.col(CPFValidator.__Col.DIG_1_WEIGTH) - 1,
        )

        # Gera o produto dos dígitos de base por seus pesos, para ambos os dígitos
        # verificadores
        df6 = df5.withColumn(
            CPFValidator.__Col.PRODUCT_DIG_1,
            df5[CPFValidator.__Col.BASE_DIGIT] * df5[CPFValidator.__Col.DIG_1_WEIGTH],
        ).withColumn(
            CPFValidator.__Col.PRODUCT_DIG_2,
            df5[CPFValidator.__Col.BASE_DIGIT] * df5[CPFValidator.__Col.DIG_2_WEIGTH],
        )
        df6.show()

        # Soma os produtos
        df7 = df6.groupBy(
            self.__docColName, CPFValidator.__Col.VERIFICATION_DIGITS
        ).agg(
            sqlfn.sum(CPFValidator.__Col.PRODUCT_DIG_1).alias(
                CPFValidator.__Col.SUM_PRODUCT_DIG_1
            ),
            sqlfn.sum(CPFValidator.__Col.PRODUCT_DIG_2).alias(
                CPFValidator.__Col.SUM_PRODUCT_DIG_2
            ),
        )

        def fixComputedDigit(colName: str) -> Column:
            col = sqlfn.col(colName)
            return sqlfn.when(col != 10, col).otherwise(0)

        # Gera os dígitos verificadores
        df8 = (
            df7.withColumn(
                CPFValidator.__Col.COMPUTED_DIG_1,
                df7[CPFValidator.__Col.SUM_PRODUCT_DIG_1] % 11,
            )
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_1,
                fixComputedDigit(CPFValidator.__Col.COMPUTED_DIG_1),
            )
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_2,
                (
                    df7[CPFValidator.__Col.SUM_PRODUCT_DIG_2]
                    + 9 * sqlfn.col(CPFValidator.__Col.COMPUTED_DIG_1)
                )
                % 11,
            )
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_2,
                fixComputedDigit(CPFValidator.__Col.COMPUTED_DIG_2),
            )
        )
        df8.show()

        # concatena os dígitos verificadores
        df9 = df8.withColumn(
            CPFValidator.__Col.COMPUTED_VERIFICATION_DIGITS,
            sqlfn.concat(
                CPFValidator.__Col.COMPUTED_DIG_1,
                CPFValidator.__Col.COMPUTED_DIG_2,
            ),
        )
        df9.show()

        # Retorna os documentos com o status de validação.
        return df9.select(
            self.__docColName,
            (
                df9[CPFValidator.__Col.VERIFICATION_DIGITS]
                == df9[CPFValidator.__Col.COMPUTED_VERIFICATION_DIGITS]
            ).alias(self.__outputColName),
        ).union(invalidRowsDf.select(self.__docColName, sqlfn.lit(False)))

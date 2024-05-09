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
        invalidDocsDf = df.filter(
            (df[self.__docColName].isin(CPFValidator._KNOWN_INVALID_DOCS))
            | (~df[self.__docColName].rlike(r"^[0-9]{11}$"))
        )

        def fixComputedDigit(colName: str) -> Column:
            col = sqlfn.col(colName)
            return sqlfn.when(col != 10, col).otherwise(0)

        computedDigverDf = (
            # Quebra o CPF em 3 partes:
            #   1. CPF completo
            #   2. Raíz (dígitos de índices 1-8), em um array
            #   3. Dígitos verificadores (digver; dígitos de índices 10-12).
            #
            # Cada dígito da raíz estará na sua própria linha.
            df.exceptAll(invalidDocsDf)
            .select(
                self.__docColName,
                sqlfn.explode(
                    sqlfn.split(df[self.__docColName].substr(1, 9), "", 9)
                ).alias(CPFValidator.__Col.BASE_DIGIT),
                df[self.__docColName]
                .substr(10, 12)
                .alias(CPFValidator.__Col.VERIFICATION_DIGITS),
            )
            # Converte cada dígito da raiz de string para int
            .withColumn(
                CPFValidator.__Col.BASE_DIGIT,
                sqlfn.col(CPFValidator.__Col.BASE_DIGIT).cast("int"),
            )
            # Cria a coluna de peso para o digver 1 (números de 1-9)
            .withColumn(
                CPFValidator.__Col.DIG_1_WEIGTH,
                sqlfn.row_number().over(
                    Window().partitionBy(self.__docColName).orderBy(self.__docColName)
                ),
            )
            # Cria as colunas de peso para o digver 2 (números de 0-8)
            # Eu sei, eu sei... o correto é de 0-9. Mas como ainda não temos o
            # primeiro dígito verificador calculado, eu vou só até o penúltimo.
            .withColumn(
                CPFValidator.__Col.DIG_2_WEIGTH,
                sqlfn.col(CPFValidator.__Col.DIG_1_WEIGTH) - 1,
            )
            # Calcula o produto da raíz pelo peso do digver 1
            .withColumn(
                CPFValidator.__Col.PRODUCT_DIG_1,
                sqlfn.col(CPFValidator.__Col.BASE_DIGIT)
                * sqlfn.col(CPFValidator.__Col.DIG_1_WEIGTH),
            )
            # Calcula o produto da raíz pelo peso do digver 2
            .withColumn(
                CPFValidator.__Col.PRODUCT_DIG_2,
                sqlfn.col(CPFValidator.__Col.BASE_DIGIT)
                * sqlfn.col(CPFValidator.__Col.DIG_2_WEIGTH),
            )
            # Soma os produtos das somas, cada um em sua coluna.
            .groupBy(self.__docColName, CPFValidator.__Col.VERIFICATION_DIGITS)
            .agg(
                sqlfn.sum(CPFValidator.__Col.PRODUCT_DIG_1).alias(
                    CPFValidator.__Col.SUM_PRODUCT_DIG_1
                ),
                sqlfn.sum(CPFValidator.__Col.PRODUCT_DIG_2).alias(
                    CPFValidator.__Col.SUM_PRODUCT_DIG_2
                ),
            )
            # Calcula o digver 1
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_1,
                sqlfn.col(CPFValidator.__Col.SUM_PRODUCT_DIG_1) % 11,
            )
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_1,
                fixComputedDigit(CPFValidator.__Col.COMPUTED_DIG_1),
            )
            # Calcula o digver 2
            # Veja que agora eu adiciono o nono peso já que tenho o digver 1
            # agora.
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_2,
                (
                    sqlfn.col(CPFValidator.__Col.SUM_PRODUCT_DIG_2)
                    + 9 * sqlfn.col(CPFValidator.__Col.COMPUTED_DIG_1)
                )
                % 11,
            )
            .withColumn(
                CPFValidator.__Col.COMPUTED_DIG_2,
                fixComputedDigit(CPFValidator.__Col.COMPUTED_DIG_2),
            )
            # Concatena os dígitos verificadores obtidos.
            .withColumn(
                CPFValidator.__Col.COMPUTED_VERIFICATION_DIGITS,
                sqlfn.concat(
                    CPFValidator.__Col.COMPUTED_DIG_1,
                    CPFValidator.__Col.COMPUTED_DIG_2,
                ),
            )
        )

        # Retorna os documentos com o status de validação.
        return computedDigverDf.select(
            self.__docColName,
            (
                computedDigverDf[CPFValidator.__Col.VERIFICATION_DIGITS]
                == computedDigverDf[CPFValidator.__Col.COMPUTED_VERIFICATION_DIGITS]
            ).alias(self.__outputColName),
        ).union(invalidDocsDf.select(self.__docColName, sqlfn.lit(False)))

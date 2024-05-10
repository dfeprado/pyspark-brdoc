import re
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

__KNOWN_INVALID_DOCS = [
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

__NON_DIGIT_CHAR_PATTERN = re.compile(r"[^0-9]+")


@udf(returnType=BooleanType())
def validateCPF(doc: str) -> bool:
    """
    Valida se um CPF Ã© vÃ¡lido conforme as regras da Receita Federal.

    O argumento `cpf` tem que ser uma string apenas de dÃ­gitos e de tamanho 11.
    """
    global __KNOWN_INVALID_DOCS
    global __NON_DIGIT_CHAR_PATTERN

    # Valida se o documento é nulo ou tem um formato inválido
    docIsNullOrHasInvalidFormat = not (
        doc is not None
        and isinstance(doc, str)
        and len(doc) == 11
        and __NON_DIGIT_CHAR_PATTERN.search(doc) is None
        and doc not in __KNOWN_INVALID_DOCS
    )
    if docIsNullOrHasInvalidFormat:
        return False

    # Faz a soma do produto dos dígitos do CPF com os pesos.
    # Faz a soma de ambos os dígitos verificadores em paralelo.
    sumDig1 = 0
    sumDig2 = 0
    w = 1
    for dig in map(lambda x: int(x), doc[:9]):
        sumDig1 += dig * w
        sumDig2 += dig * (w - 1)
        w += 1

    # Calcula e verifica o primeiro dígito verificador.
    dig1 = sumDig1 % 11
    if dig1 == 10:
        dig1 = 0
    if doc[9] != str(dig1):
        return False

    # Calcula e verifica o segundo dígito verificador.
    dig2 = (sumDig2 + dig1 * 9) % 11
    if dig2 == 10:
        dig2 = 0
    if doc[10] != str(dig2):
        return False

    return True

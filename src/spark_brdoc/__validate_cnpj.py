import re
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

__KNOWN_INVALID_DOCS = [
    "00000000000000",
    "11111111111111",
    "22222222222222",
    "33333333333333",
    "44444444444444",
    "55555555555555",
    "66666666666666",
    "77777777777777",
    "88888888888888",
    "99999999999999",
]

__NON_DIGIT_CHARS_PATTERN = re.compile(r"[^0-9]+")


@udf(returnType=BooleanType())
def validateCNPJ(doc: str) -> bool:
    """Valida um CNPJ.

    Args:
        doc (`str`): O documento a ser validado.

    Returns:
        `True` se o documento for válido, `False` caso contrário. Um documento
        inválido é aquele que:
         - É nulo; ou
         - não é uma string; ou
         - não tem 14"""

    global __KNOWN_INVALID_DOCS
    global __NON_DIGIT_CHARS_PATTERN

    # Pré-valida a nulidade e o formato do documento
    docIsNullOrHasInvalidFormat = not (
        doc is not None
        and isinstance(doc, str)
        and len(doc) == 14
        and __NON_DIGIT_CHARS_PATTERN.search(doc) is None
        and doc not in __KNOWN_INVALID_DOCS
    )
    if docIsNullOrHasInvalidFormat:
        return False

    # Inicia a soma do produto dos dígitos do documento pelos pesos.
    # Faz a soma de ambos os dígitos paralelamente.
    sumDig1 = 0
    sumDig2 = 0
    w = 5
    for dig in map(lambda x: int(x), doc[:12]):
        sumDig2 += dig * (w + 1)

        if w == 1:
            w = 9

        sumDig1 += dig * w

        w -= 1

    # Cálculo e comparação do primeiro dígito verificador.
    restDig1 = sumDig1 % 11
    dig1 = 11 - restDig1 if restDig1 > 1 else 0
    if doc[12] != str(dig1):
        return False

    # Cálculo e comparação do segundo dígito verificador.
    restDig2 = (sumDig2 + 2 * dig1) % 11
    dig2 = 11 - restDig2 if restDig2 > 1 else 0
    if doc[13] != str(dig2):
        return False

    return True

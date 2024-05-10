SparkBRDocs
===
Utilitários para validação de documentos do Brasil usando Apache Spark pySpark.

# Instalação
TODO

# Exemplos de uso
## Validando CPF/CNPJ
```python
import spark_brdocs

df = spark.createDataFrame([
    (1, "40116886064"), # CPF válido, gerado através da 4devs.com.br, sem pontuação (obrigatório).
    (2, "50116886064"), # CPF inválido, cópia do primeiro com alteração do primeiro dígito.
    (3, "abcdefgh"), # Formato inválido.
    (4, None), # Documento nulo também é considerado inválido.
], schema="id int, cpf string")

# a função spark_brdocs.validateCNPJ() faz a validação de CNPJ.
df2 = df.withColumn("is_valid", spark_brdocs.validateCPF("cpf")) # Cria a coluna booleana is_valid.

assert df2.filter(df2["is_valid"] == True).count() == 1
assert df2.filter(df2["is_valid"] == False).count() == 3
```

# Documentação
TODO
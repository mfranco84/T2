from funciones import definir_esquema_json, transformar_a_esquema_parquet, generar_df_articulos_por_mes_anho, generar_df_research_group
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, NullType

spark = SparkSession.builder.appName("UnitTest").getOrCreate()

def test_transformar_a_esquema_parquet():
    esquema_inicial = definir_esquema_json()
    articulos = [
        (
            "ok",
            "work",
            "1.0.0",
            (
                "10.1101/2020.01.24.915157",
                [(
                    "https://orcid.org/0000-0002-9697-0962",
                    [("Inst A",)],
                    True,
                    "Zhang",
                    "Ying",
                    "Ying Zhang",
                    "additional",
                    "Jr."
                )],
                [("Inst A",)],
                "Bioinformatics",
                ("2020-01-24T15:45:13Z",),
                "10.64898",
                [(
                    "10.1002/jmv.25678",
                    "ISSN",
                    "Model parameters and outbreak control for SARS",
                    "Zhang Y, Deng Y, Li Y, et al.",
                    "publisher",
                    "1160",
                    "issn-type",
                    "1",
                    "Emerging infectious diseases",
                    "2020110412100640000_2020.01.23.20018549v2.9",
                    "European Centre for Disease...",
                    "10",
                    "volume-title",
                    "2020"
                )],
                [(
                    "https://syndication.highwire.org/content/doi/10.1101/2020.01.23.20018549",
                    "application/pdf",
                    "version1",
                    "similarity-checking"
                )],
                "preprint"
            )
        )
    ]
    df_inicial = spark.createDataFrame(articulos, schema=esquema_inicial)
    df_transformado = transformar_a_esquema_parquet(df_inicial)

    columnas_final = ["doi", "author", "inst", "groupTitle", "createdDate", "prefix", "reference", "link", "subtype"]
    assert df_transformado.columns == columnas_final

    datos = df_transformado.collect()
    assert datos[0]["doi"] == "10.1101/2020.01.24.915157"
    assert datos[0]["groupTitle"] == "Bioinformatics"
    assert datos[0]["createdDate"] == "2020-01-24T15:45:13Z"

def test_generar_df_articulos_por_mes_anho():
    filas = [
        ("10.1101/2020.01.24.915157", "Author1", "Inst1", "Group1", "2020-01-24T15:45:13Z", "10.64898", "Reference1"),
        ("10.3201/eid1007.030647", "Author2", "Inst1", "Group1", "2022-01-24T11:32:09Z", "13.53789", "Reference2"),
        ("10.1056/NEJMoa2001316", "Author1", "Inst1", "Group1", "2020-09-11T18:28:24Z", "11.13285", "Reference1"),
        ("10.1101/2020.01.24.915157", "Author1", "Inst1", "Group1", "2020-01-24T15:45:13Z", "10.64898", "Reference1"),
        ("10.1002/jmv.25678", "Author5", "Inst5", "Group1", "2020-01-24T15:45:13Z", "10.64898", "Reference1"),
        ("10.3201/eid1007.030647", "Author3", "Inst4", "Group1", "2022-07-14T13:15:05Z", "16.95638", "Reference3"),
    ]
    columnas = ["doi", "author", "inst", "groupTitle", "createdDate", "prefix", "reference"]

    df_inicial = spark.createDataFrame(filas, columnas)
    df_resultado = generar_df_articulos_por_mes_anho(df_inicial)

    assert df_resultado.columns == ["created_year", "created_month", "total_articles"]
    assert df_resultado.count() == 3
    assert df_resultado.collect()[0]["created_year"] == 2020
    assert df_resultado.collect()[0]["created_month"] == 1
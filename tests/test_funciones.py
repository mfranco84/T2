from funciones import definir_esquema_json, transformar_a_esquema_parquet, generar_df_articulos_por_mes_anho, generar_df_research_group, generar_df_research_areas_per_person, generar_df_person_references, generar_df_person_most_references, generar_df_percentil
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

def test_generar_df_research_group():
    filas = [
        ("Group1", "10.1101/2020.01.24.915157"),
        ("Group2", "10.3201/eid1007.030647"),
        ("Group1", "10.3201/eid1007.030647"),
        ("Group3", "10.1056/NEJMoa2001316"),
        ("Group2", "10.1101/2020.01.24.915157"),
        ("Group2", "10.1002/jmv.25678"),
        ("Group2", "10.3201/eid1007.030647"),
    ]
    columnas = ["groupTitle", "doi"]
    df_inicial = spark.createDataFrame(filas, columnas)
    df_resultado = generar_df_research_group(df_inicial).orderBy(col("group_title"))

    assert df_resultado.columns == ["group_title", "total"]
    assert df_resultado.count() == 3
    assert df_resultado.collect()[0]["group_title"] == "Group1"
    assert df_resultado.collect()[0]["total"] == 2
    assert df_resultado.collect()[1]["group_title"] == "Group2"
    assert df_resultado.collect()[1]["total"] == 3

def test_generar_df_research_areas_per_person():
    schema = StructType([
        StructField("doi", StringType(), True),
        StructField("groupTitle", StringType(), True),
        StructField("author", ArrayType(StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True)
        ])), True)
    ])
    
    filas = [
        ("doi1", "Physics", [{"given": "Isaac", "family": "Newton"}]),
        ("doi2", "Math", [{"given": "Isaac", "family": "Newton"}]),
        ("doi3", "Physics", [{"given": "Marie", "family": "Curie"}]),
        ("doi4", "Chemistry", [{"given": "", "family": ""}]), # unknown
    ]
    
    df_inicial = spark.createDataFrame(filas, schema=schema)
    df_resultado = generar_df_research_areas_per_person(df_inicial)
    
    datos = df_resultado.collect()
    # Newton has 2 areas, Curie 1, Unknown 1
    assert datos[0]["full_name"] == "IsaacNewton"
    assert datos[0]["total"] == 2
    assert "Physics" in datos[0]["research_areas"]
    assert "Math" in datos[0]["research_areas"]
    assert any(r["full_name"] == "unknown" for r in datos)

def test_generar_df_person_references():
    schema = StructType([
        StructField("doi", StringType(), True),
        StructField("author", ArrayType(StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("reference", ArrayType(StructType([
            StructField("DOI", StringType(), True)
        ])), True)
    ])
    
    filas = [
        ("doi1", [{"given": "Isaac", "family": "Newton", "name": None}], [{"DOI": "ref1"}, {"DOI": "ref2"}]),
        ("doi1", [{"given": "Isaac", "family": "Newton", "name": None}], [{"DOI": "ref1"}]), # Duplicate ref in same paper
        ("doi2", [{"given": "Isaac", "family": "Newton", "name": None}], [{"DOI": "ref3"}]),
        ("doi3", [{"given": None, "family": None, "name": ""}], [{"DOI": "ref4"}]), # unknown
    ]
    
    df_inicial = spark.createDataFrame(filas, schema=schema)
    df_resultado = generar_df_person_references(df_inicial)
    
    # Newton: 2 unique refs from doi1 + 1 from doi2 = 3
    # Unknown: excluded
    assert df_resultado.count() == 1
    datos = df_resultado.collect()
    assert datos[0]["full_name"] == "IsaacNewton"
    assert datos[0]["total_references"] == 3

def test_generar_df_person_most_references():
    schema = StructType([
        StructField("doi", StringType(), True),
        StructField("author", ArrayType(StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("reference", ArrayType(StructType([
            StructField("DOI", StringType(), True)
        ])), True)
    ])
    
    filas = [
        ("doi1", [{"given": "Isaac", "family": "Newton", "name": None}], [{"DOI": "ref1"}]),
        ("doi2", [{"given": "Marie", "family": "Curie", "name": None}], [{"DOI": "ref2"}, {"DOI": "ref3"}]),
    ]
    
    df_inicial = spark.createDataFrame(filas, schema=schema)
    df_resultado = generar_df_person_most_references(df_inicial)
    
    assert df_resultado.count() == 1
    assert df_resultado.collect()[0]["full_name"] == "MarieCurie"

def test_generar_df_percentil():
    schema = StructType([
        StructField("doi", StringType(), True),
        StructField("author", ArrayType(StructType([
            StructField("given", StringType(), True),
            StructField("family", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("reference", ArrayType(StructType([
            StructField("DOI", StringType(), True)
        ])), True)
    ])
    
    filas = [
        ("d1", [{"given": "A", "family": "A", "name": None}], [{"DOI": "r1"}]), # 1 ref
        ("d2", [{"given": "B", "family": "B", "name": None}], [{"DOI": "r2"}, {"DOI": "r3"}]), # 2 refs
        ("d3", [{"given": "C", "family": "C", "name": None}], [{"DOI": "r4"}, {"DOI": "r5"}, {"DOI": "r6"}]), # 3 refs
    ]
    
    df_inicial = spark.createDataFrame(filas, schema=schema)
    # Median (0.5 percentile) of [1, 2, 3] is 2
    df_resultado = generar_df_percentil(df_inicial, 0.5)
    
    assert df_resultado.collect()[0]["total_references"] == 2

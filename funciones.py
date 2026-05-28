from pyspark.sql import functions as Func
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, LongType

def definir_esquema_json():
    esquema_autor = StructType([
        StructField("ORCID", StringType(), True),
        StructField("affiliation", ArrayType(StructType([
            StructField("name", StringType(), True)
        ])), True),
        StructField("authenticated-orcid", BooleanType(), True),
        StructField("family", StringType(), True),
        StructField("given", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("suffix", StringType(), True)
    ])

    esquema_reference = StructType([
        StructField("DOI", StringType(), True),
        StructField("ISSN", StringType(), True),
        StructField("article-title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("doi-asserted-by", StringType(), True),
        StructField("first-page", StringType(), True),
        StructField("issn-type", StringType(), True),
        StructField("issue", StringType(), True),
        StructField("journal-title", StringType(), True),
        StructField("key", StringType(), True),
        StructField("unstructured", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("volume-title", StringType(), True),
        StructField("year", StringType(), True)
    ])

    esquema_link = StructType([
        StructField("URL", StringType(), True),
        StructField("content-type", StringType(), True),
        StructField("content-version", StringType(), True), 
        StructField("intended-application", StringType(), True)
    ])

    esquema_mensaje = StructType([
        StructField("DOI", StringType(), True),
        StructField("author", ArrayType(esquema_autor), True),
        StructField("institution", ArrayType(StructType([
            StructField("name", StringType(), True)
        ])), True),
        StructField("group-title", StringType(), True),
        StructField("created", StructType([
            StructField("date-time", StringType(), True)
        ]), True),
        StructField("prefix", StringType(), True),
        StructField("reference", ArrayType(esquema_reference), True),
        StructField("link", ArrayType(esquema_link), True),
        StructField("subtype", StringType(), True)
    ])

    return StructType([
    StructField("status", StringType(), True),
    StructField("message-type", StringType(), True),
    StructField("message-version", StringType(), True),
    StructField("message", esquema_mensaje, True)
])

def transformar_a_esquema_parquet(df):
    """
    Transforms the raw Crossref JSON DataFrame to the required Parquet schema.
    Flattens the 'message' struct and selects/renames specific fields.
    """

    msg_df = df.select("message.*")

    return msg_df.select(
        Func.col("DOI").alias("doi"),
        Func.col("author"),
        Func.col("institution").alias("inst"),
        Func.col("group-title").alias("groupTitle"),
        Func.col("created.date-time").alias("createdDate"),
        Func.col("prefix"),
        Func.col("reference"),
        Func.col("link"),
        Func.col("subtype")
    )

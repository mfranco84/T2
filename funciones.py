from pyspark.sql.functions import col, to_date, year, month, to_utc_timestamp, desc, explode, concat, coalesce, lit, trim, when, collect_set, size, percentile_approx
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
    msg_df = df.select("message.*")

    return msg_df.select(
        col("DOI").alias("doi"),
        col("author"),
        col("institution").alias("inst"),
        col("group-title").alias("groupTitle"),
        col("created.date-time").alias("createdDate"),
        col("prefix"),
        col("reference"),
        col("link"),
        col("subtype")
    )

def generar_df_articulos_por_mes_anho(df):
    df_year_month = (
        # df.select("doi", "createdDate").distinct()
        df.dropDuplicates(["doi"]) # Se procede a eliminar duplicados por DOI, aunque sea en fechas distintas.
        # df.withColumn("createdDateStruct", to_date(col("createdDate"), "dd-MM-yyyy"))
        .withColumn("createdDateUTC", to_utc_timestamp(col("createdDate"), "UTC"))
        .withColumn("created_year", year(col("createdDateUTC")))
        .withColumn("created_month", month(col("createdDateUTC")))
        # .select("created_year", "created_month", "total_articles")
    )
    return (
        df_year_month.groupBy("created_year", "created_month")
        .count().withColumnRenamed("count", "total_articles")
        # .orderBy("total_articles", ascending=False)
        .orderBy("created_year", "created_month")
    )

def generar_df_research_group(df):
    df_seleccionado = df.select("groupTitle", "doi").distinct()
    return (
        df_seleccionado.groupBy(col("groupTitle").alias("group_title"))
        .count()
        .withColumnRenamed("count", "total")
    )

def generar_df_research_areas_per_person(df):
    df_auth = df.select("doi", "groupTitle", "author").where(col("author").isNotNull())
    df_exp = df_auth.withColumn("author", explode(col("author"))).where(col("groupTitle").isNotNull())

    df_names = (
        df_exp.withColumn("full_name_raw", concat(coalesce(col("author.given"), lit("")), coalesce(col("author.family"), lit(""))))
        .withColumn("full_name_trimmed", trim(col("full_name_raw")))
        .withColumn("full_name", when(col("full_name_trimmed") == "", lit("unknown")).otherwise(col("full_name_trimmed")))
    )

    df_grouped = (
        df_names.groupBy("full_name").agg(collect_set(col("groupTitle")).alias("research_areas"))
    )

    df_final = df_grouped.withColumn("total", size(col("research_areas"))).orderBy(col("total").desc())
    return df_final


def generar_df_person_references(df):
    df_auth = df.select("doi", "author", "reference").where(col("author").isNotNull())
    df_exp = df_auth.withColumn("author", explode(col("author")))

    df_names = (
        df_exp.withColumn("full_name_raw", concat(coalesce(col("author.given"), lit("")), coalesce(col("author.family"), lit(""))))
        .withColumn("full_name_raw", when(trim(col("full_name_raw")) == "", trim(coalesce(col("author.name"), lit("")))).otherwise(col("full_name_raw")))
        .withColumn("full_name", when(trim(col("full_name_raw")) == "", lit("unknown")).otherwise(trim(col("full_name_raw"))))
    )

    df_refs = (
        df_names
        .select("full_name", "doi", explode(col("reference")).alias("reference"))
        .withColumn("reference_doi", col("reference.DOI"))
        .where(col("reference_doi").isNotNull())
        .where(trim(col("reference_doi")) != "")
        .dropDuplicates(["full_name", "doi", "reference_doi"])
    )

    return (
        df_refs.where(col("full_name") != "unknown")
        .groupBy("full_name")
        .count()
        .withColumnRenamed("count", "total_references")
    )


def generar_df_person_most_references(df):
    return (
        generar_df_person_references(df)
        .orderBy(col("total_references").desc())
        .limit(1)
    )


def generar_df_percentil(df, valor_percentil):

    df_person_refs = generar_df_person_references(df)

    return (
        df_person_refs
        .agg(
            percentile_approx(
                col("total_references"),
                lit(valor_percentil)
            ).alias("total_references")
        )
    )
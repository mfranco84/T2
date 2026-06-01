import sys
import os
from pyspark.sql import SparkSession
from funciones import (
  definir_esquema_json,
  transformar_a_esquema_parquet,
  generar_df_articulos_por_mes_anho,
  generar_df_research_group,
  generar_df_research_areas_per_person,
  generar_df_person_most_references,
  generar_df_percentil
)

ROJO = "\033[31m"
VERDE = "\033[32m"
AMARILLO = "\033[33m"
AZUL = "\033[34m"
RESET = "\033[0m"

def validar_argumentos():
  if len(sys.argv) < 2:
    print(f"{ROJO}\n[ERROR] Hace falta un directorio de archivos JSON.{RESET}")
    print(f"{AMARILLO}Uso correcto:")
    print(f"    spark-submit programaestudiante.py <directorio_archivos_json>{RESET}")
    return False
  return True

def ruta_existe(spark, path):
    """
    Verifica si la ruta existe en el sistema de archivos de Spark (Local, HDFS, S3, etc.)
    """
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

def main():
    if not validar_argumentos():
      sys.exit(1)

    path_archivos_json = sys.argv[1:]
    parquet_output_path = "output/crossref-parquet"
    csv_articles_month_year_output_path = "output/articles_month_year/articles_month_year.csv"
    csv_research_areas_per_person_output_path = "output/research_group/research_group.csv"
    
    spark = (SparkSession.builder
        .appName("Tarea2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN") # Tratando de evitar logs de INFO... no funciona del todo, pero ayuda un poco

    # Punto 1 (Solo crear el parquet si no existe, sino cargarlo directamente)
    if ruta_existe(spark, parquet_output_path):
      print(f"{AZUL}Cargando datos desde archivo parquet...{RESET}")
      df = spark.read.parquet(parquet_output_path)
    else:
      print(f"{VERDE}Leyendo archivos json...{RESET}")
      esquema_json = definir_esquema_json()
      df_original = spark.read.option("multiline", "true").schema(esquema_json).json(path_archivos_json)
      print(f"{VERDE}Esquema original:{RESET}")
      df_original.printSchema()
      print(f"{VERDE}Mostrando datos del esquema original:{RESET}")
      df_original.show(5, truncate=False)
      df_esquema_parquet = transformar_a_esquema_parquet(df_original)
      df_esquema_parquet.write.mode("overwrite").parquet(parquet_output_path)
      df = spark.read.parquet(parquet_output_path)

    print(f"{VERDE}Esquema parquet:{RESET}")
    df.printSchema()
    print(f"{VERDE}Mostrando datos del esquema parquet:{RESET}")
    df.show(5, truncate=True)

    # Punto 2 (Generar el archivo csv)
    df_articulos_por_mes_anho = generar_df_articulos_por_mes_anho(df)
    df_articulos_por_mes_anho.printSchema()
    df_articulos_por_mes_anho.show(5, truncate=True)
    df_articulos_por_mes_anho.write.mode("overwrite").option("header", "true").csv(csv_articles_month_year_output_path)

    # Punto 3 (Generar el archivo csv)
    df_research_group = generar_df_research_group(df)
    df_research_group.printSchema()
    df_research_group.show(5, truncate=True)
    df_research_group.write.mode("overwrite").option("header", "true").csv(csv_research_areas_per_person_output_path)

    # Punto 4: Generar JSON con las áreas de investigación por persona
    json_research_areas_output_path = "output/research_areas_per_person"
    print(f"{VERDE}Generando archivo JSON de research areas por persona...{RESET}")
    df_research_areas = generar_df_research_areas_per_person(df)
    df_research_areas.printSchema()
    df_research_areas.show(5, truncate=False)
    # Escribir un único archivo JSON (coalesce 1)
    df_research_areas.coalesce(1).write.mode("overwrite").json(json_research_areas_output_path)

    # Punto 5: Generar JSON con la persona que tiene más referencias con DOI
    json_person_references_output_path = "output/person_most_references"
    print(f"{VERDE}Generando archivo JSON de la persona con más referencias con DOI...{RESET}")
    df_person_references = generar_df_person_most_references(df)
    df_person_references.printSchema()
    df_person_references.show(5, truncate=False)
    df_person_references.coalesce(1).write.mode("overwrite").json(json_person_references_output_path)

    # Punto 6: Generar CSV con el percentil 25 de total_references
    csv_percentil_25_output_path = "output/percentil_25"
    print(f"{VERDE}Generando archivo CSV del percentil 25 de total_references...{RESET}")
    df_percentil_25 = generar_df_percentil(df, 0.25)
    df_percentil_25.printSchema()
    df_percentil_25.show(5, truncate=False)
    df_percentil_25.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_percentil_25_output_path)

    # Punto 7: Generar CSV con el percentil 50 de total_references
    csv_percentil_50_output_path = "output/percentil_50"
    print(f"{VERDE}Generando archivo CSV del percentil 50 de total_references...{RESET}")
    df_percentil_50 = generar_df_percentil(df, 0.5)
    df_percentil_50.printSchema()
    df_percentil_50.show(5, truncate=False)
    df_percentil_50.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_percentil_50_output_path)

    # Punto 8: Generar CSV con el percentil 75 de total_references
    csv_percentil_75_output_path = "output/percentil_75"
    print(f"{VERDE}Generando archivo CSV del percentil 75 de total_references...{RESET}")
    df_percentil_75 = generar_df_percentil(df, 0.75)
    df_percentil_75.printSchema()
    df_percentil_75.show(5, truncate=False)
    df_percentil_75.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_percentil_75_output_path)

    ######################################################

if __name__ == "__main__":
    main()

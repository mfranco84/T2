import sys
import os
from pyspark.sql import SparkSession
from funciones import (
  definir_esquema_json,
  transformar_a_esquema_parquet
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
    
    spark = (SparkSession.builder
        .appName("Tarea2")
        .getOrCreate()
    )

    if ruta_existe(spark, parquet_output_path):
      print(f"{AZUL}Cargando datos desde archivo parquet...{RESET}")
      df = spark.read.parquet(parquet_output_path)
    else:
      print(f"{VERDE}Leyendo archivos json...{RESET}")
      esquema_json = definir_esquema_json()
      df_original = spark.read.option("multiline", "true").schema(esquema_json).json(path_archivos_json)
      print(f"{VERDE}Esquema original:{RESET}")
      df_original.printSchema()
      df_original.show(5, truncate=False)
      df_esquema_parquet = transformar_a_esquema_parquet(df_original)
      df_esquema_parquet.write.mode("overwrite").parquet(parquet_output_path)
      df = spark.read.parquet(parquet_output_path)

    print(f"{VERDE}Esquema parquet:{RESET}")
    df.printSchema()
    df.show(5, truncate=True)
    
    sys.exit(1)
    ######################################################

if __name__ == "__main__":
    main()

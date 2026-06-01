# Tarea #2 - Procesamiento de datos

Procesamiento de datos y creacion de archivos tipo parquet.

### Estudiantes:
- Franco Rivera Carranza
- Sebastián Rodríguez
- Juan Pablo Arce

## Instrucciones de Ejecución

### Como primer pre-requisito se debe haber instalado el contenedor configurado para la lección 2, el cual se encuentra en la ruta L2/spark. Se ejcuta el comando:

```bash
docker build --tag bigdatafull .
```

### Como segundo pre-requisito se debe copiar todo el contenido de archivos json `crossref/crossref/` que previamente fue descomprimido como parte de la lección 2, a la carpeta de la tarea, dentro del directorio `crossref/` (crear directorio, ya que no existe). El directorio de la tarea quedaría de esta manera:

```bash
T2
 |-- crossref/
    |-- 10.1101_2020.01.19.911669.json
    |-- 10.1101_2020.01.20.913368.json
    |-- 10.1101_2020.01.21.914044.json
    |-- ........
 |-- tests/
    |-- test_funciones.py
 |-- .gitignore
 |-- conftest.py
 |-- funciones.py
 |-- programaestudiante.py
 |-- pytest.ini
 |-- README.md
```


### Para montar el contenido de la tarea dentro del contenedor. Ingresar a la carpeta de la tarea y ejecutar el siguiente comando:

```bash
docker run -p 8888:8888 -it -v ".:/bg" bigdatafull /bin/bash
```


### Ejecutar Programa Principal
El programa procesa archivos JSON multilínea y genera resultados en la carpeta `output/`. Se requiere incrementar el uso de memoria para poder procesar la extracción y filtración de todos los datos.

```bash
cd /bg
spark-submit  --driver-memory 4g  --executor-memory 4g  programaestudiante.py crossref/*.json
```

### Para ejecutar las pruebas:

```bash
cd /bg
pytest tests/ -v
```

## Resultados Esperados (Carpeta `output/`)
1. `crossref-parquet/`: Datos convertidos al formato Parquet.
2. `articles_month_year/`: CSV con conteo de artículos por mes y año.
3. `research_group/`: CSV con conteo de artículos por grupo de investigación.
4. `research_areas_per_person/`: JSON con áreas de investigación por persona.
5. `person_most_references/`: JSON con la persona que tiene más referencias con DOI.
6. `percentil_25/`, `percentil_50/`, `percentil_75/`: CSVs con los valores de los percentiles de referencias.

# Tarea #2 - Procesamiento de datos

Procesamiento de datos y creacion de archivos tipo parquet.


## Instrucciones de Ejecución

### Como pre-requisito se debe haber instalado el contenedor configurado para la lección 2, el cual se encuentra en la ruta L2/spark. Se ejcuta el comando:

```bash
docker build --tag bigdatafull .
```

### Para montar el contenido de la tarea dentro del contenedor. Ingresar a la carpeta de la tarea y ejecutar el siguiente comando:

```bash
docker run -p 8888:8888 -it -v ".:/bg" bigdatafull /bin/bash
```


### Ejecutar Programa Principal
El programa procesa archivos JSON multilínea y genera resultados en la carpeta `output/`.

```bash
cd /bg
spark-submit programaestudiante.py crossref/*.json
```

### Para ejecutar las pruebas:

```bash
cd /bg
pytest tests/ -v
```

## Resultados Esperados (Carpeta `output/`)
1. `crossref-parquet/`: Datos convertidos al formato Parquet.

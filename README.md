# Ingesta proyecto parcial Universidad

## Contenidos:

### General:

- `crear_mv_ingesta.yaml`: Plantilla para crear máquina virtual de ingesta con CloudFormation.

- `data_catalogs\`: contiene los data catalogs para AWS Glue

### api-cursos
    
- `ingestion-mongodb/ingesta.py`: obtener la data de la base de datos Mongo en MV Bases de datos y escribirla en archivos `.json`. Luego mandar los jsons a un bucket de S3.

- `ingestion-mysql/ingesta.py`: obtener la data de la base de datos MySQL en MV Bases de datos y escribirla en archivos `.csv`. Luego mandar los csvs a un bucket de S3.

- `flatten_official.ipynb`: notebook para transformar los datos ingeridos a un modelo relacional, con el objetivo de realizar consultas. 

> **Tener en cuenta:** Dentro del bucket de S3, deben haber carpetas para cada entidad de las bases de datos. Por ejemplo, `profesores.csv` debería guardarse dentro de la carpeta `profesores/`
import psycopg2
import boto3
import csv

bucketname = "proyecto-parcial-out"
tables = ["estudiante", "beca"]
files = ["estudiantes.csv", "becas.csv"]

def get_db_connection():
    return psycopg2.connect(
        host="your host",
        port="your port",
        user="user",
        password="password",
        database="database"
    )

def write_table_to_file(conn, table, outfile):
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table + ";")
    results = cur.fetchall()

    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([col[0] for col in cur.description])
        writer.writerows(results)

    cur.close()

def upload_to_s3(filenames, bucketname):
    s3 = boto3.client('s3')
    for filename in filenames:
        subdir = filename.split(".")[0] + "/"
        response = s3.upload_file(filename, bucketname, subdir + filename)
        print(response)
    print("Ingesta PostgreSQL completada")
# Reference:
# https://medium.com/@jewelski/connect-to-a-postgresql-database-using-psycopg2-and-export-data-474f0f3a3f70

if __name__ == "__main__":
    success = False
    try:
        conn = get_db_connection()
        print("Conexión a la base de datos exitosa")
        for i in range(len(tables)):
            write_table_to_file(conn, tables[i], files[i])
        success = True
    except psycopg2.Error as e:
        print("Error conectando a la base de datos:")
        print(e)
    finally:
        if conn:
            conn.close()
            print("Se cerró la conexión a la base de datos PostgreSQL correctamente")
    if success:
        upload_to_s3(files, bucketname)
    else:
        print("Ingesta PostgreSQL no completada")

    

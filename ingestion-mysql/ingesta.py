import mysql.connector
import csv 
import boto3

mydb = mysql.connector.connect(
    host="your-host",
    port=8005,
    user="root",
    password="your-password",
    database="your-database"
)
print(mydb)
bucketname = "proyecto-parcial-out"
tables = ["Profesor", "Dicta"]
files = ["profesores.csv", "dicta.csv"]

def write_table_to_file(table, outfile):
    query = "SELECT * FROM " + table + ";"
    try:
        cursor = mydb.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        with open(outfile, 'w', newline='') as file:
            myFile = csv.writer(file)
            myFile.writerows(rows)
        print("Table " + table + "successfully written as csv.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
# query_profesores='SELECT * FROM Profesor;'
# cursor = mydb.cursor()
# cursor.execute(query_profesores)
# rows = cursor.fetchall()
# file1 = open(files[0], 'w')
# myFile = csv.writer(file1)
# myFile.writerows(rows)
# file1.close()

def upload_to_s3(filenames, bucketname):
    s3 = boto3.client('s3')
    for filename in filenames:
        subdir = filename.split(".")[0] + "/"
        response = s3.upload_file(filename, bucketname, subdir + filename)
        print(response)
    print("Ingesta completada")

if __name__ == "__main.py__":
    for i in range(len(tables)):
        write_table_to_file(table=tables[i], outfile=files[i])
    upload_to_s3(files, bucketname)
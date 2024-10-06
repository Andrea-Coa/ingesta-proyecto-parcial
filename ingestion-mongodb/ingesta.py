from pymongo import MongoClient
import json
import boto3

mongodb_url = "mongodb://172.31.47.47:27017/universidad"

# todo esto estará dentro de un directorio output_data en la mv ingesta
dir = "output_data/"
files = ["cursos.json", "inscripcions.json", "ofertacursos.json"] 

# bucket de S3 para guardar los datos ingeridos
bucketname = "proyecto-parcial-out"

def retrieve_data_from_collection_with_horarios(collection_name, outfile):
    client = MongoClient(mongodb_url)
    db = client.universidad    
    collection = db[collection_name]

    documents = collection.find()
    with open(outfile, "w") as writefile:
        for doc in documents:
            # print(doc)
            doc["_id"] = str(doc["_id"])
            for horario in doc["horarios"]:
                horario["_id"] = str(doc["_id"])
            json.dump(doc, writefile)
            writefile.write("\n")

    print("Finished data retrieval from " + collection_name + " successfully.")

def retrieve_data_from_cursos():
    client = MongoClient(mongodb_url)
    db = client.universidad    
    collection = db.cursos
    documents = collection.find()
    with open(dir + files[0], "w") as writefile:
        for doc in documents:
            # print(doc)
            doc["_id"] = str(doc["_id"])
            json.dump(doc, writefile)
            writefile.write("\n")
    print("Finished data retrieval from cursos successfully.")

def upload_to_s3(filenames, bucketname):
    s3 = boto3.client('s3')
    for filename in filenames:
        subdir = filename.split(".")[0] + "/"
        response = s3.upload_file(dir + filename, bucketname, subdir + filename)
        print(response)
    print("Ingesta completada")

if __name__ == "__main__":
    retrieve_data_from_cursos()
    retrieve_data_from_collection_with_horarios("inscripcions", dir + files[1])
    retrieve_data_from_collection_with_horarios("ofertacursos", dir + files[2])

    upload_to_s3(files, bucketname)


# RESOURCES
# https://medium.com/@stevencibambo/loading-data-from-mongodb-with-pymongo-for-analysis-c0f61a8538a0

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession

urls = {
    "gares" : "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/records?limit=-1",
    "lignes" : "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/lignes-par-type/records?limit=-1",
}

spark = SparkSession.builder.master('local[*]').appName('TP_cloud').getOrCreate()

for key, value in urls.items():
    print(f"Get {key} data from offset 0")
    data = []
    response = requests.get(value).json()
    data += response["results"]
    offset_number = response["total_count"]
    current_offset = 100
    while current_offset < offset_number and current_offset < 9900:
        print(f"Get {key} data from offset {current_offset}")
        response = requests.get(value + f"&offset={current_offset}").json()
        data += response["results"]
        current_offset += 100

    file_path = f"/tmp/{key}_data.json"
    print(f"Save {key} data in local {file_path}")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    date = datetime.now().isoformat().split("T")[0]
    hdfs_file_path = f"source_data/{date}/raw/{key}.json"
    print(f"Save {key} data in HDFS {hdfs_file_path}")
    df = spark.read.json(file_path, multiLine=True)
    df.write.mode('overwrite').json(f"hdfs://localhost:9000/{hdfs_file_path}")

key = "objets-trouves"
file_path = f"/home/ubuntu/Documents/PR6/objets-trouves-restitution.json"
hdfs_file_path = f"source_data/{date}/raw/{key}.json"
print(f"Save {key} data in HDFS {hdfs_file_path}")
df = spark.read.json(file_path, multiLine=True)
df.write.mode('overwrite').json(f"hdfs://localhost:9000/{hdfs_file_path}")

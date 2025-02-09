from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, concat_ws

date = datetime.now().isoformat().split("T")[0]

spark = SparkSession.builder.master('local[*]').appName('TP_cloud').getOrCreate()

reading_path = f"source_data/{date}/clean/"
writing_path = f"source_data/{date}/aggregate/"

print(f"Transforming Gares data")
df_gares = spark.read.parquet(f"hdfs://localhost:9000/{reading_path}/gares_locations.parquet")

df_gares=df_gares.withColumn("latitude", col("geo_point_2d.lat"))
df_gares=df_gares.withColumn("longitude", col("geo_point_2d.lon"))
df_gares=df_gares.drop("geo_point_2d")

df_gares = df_gares.withColumn("type", when((col("fret")==True) & (col("voyageurs")==True), "Fret et voyageurs")
                                        .when(col("fret")==True, "Fret")
                                        .when(col("voyageurs")==True, "Voyageurs")
                                        .otherwise("Inconnu"))

df_gares.write.mode('overwrite').option("header", True).csv(f"hdfs://localhost:9000/{writing_path}/gares_with_longitude_latitude.csv")
df_gares.write.mode('overwrite').option("header", True).csv(f"hdfs://localhost:9000/power_bi/gares_with_longitude_latitude.csv")

print(f"Transforming Lignes data")
df_lignes = spark.read.parquet(f"hdfs://localhost:9000/{reading_path}/lignes_shapes.parquet")
df_lignes=df_lignes.withColumn("c_geo_f", concat_ws(",", col("c_geo_f.lat"), col("c_geo_f.lon")))


rows = []

for ligne in df_lignes.collect():
    libelle_ligne = ligne["lib_ligne"]
    type_ligne = ligne["type_ligne"]
    index=1

    depart_latitude = ligne["c_geo_d"].split(",")[0]
    depart_longitude = ligne["c_geo_d"].split(",")[1]
    rows.append(Row(libelle_ligne, type_ligne, index, depart_latitude, depart_longitude))
    index += 1
    
    for point in ligne["coordinates"]:
        rows.append(Row(libelle_ligne, type_ligne, index, point[1], point[0]))
        index += 1

    arrive_latitude = ligne["c_geo_f"].split(",")[0]
    arrive_longitude = ligne["c_geo_f"].split(",")[1]
    rows.append(Row(libelle_ligne, type_ligne, index, arrive_latitude, arrive_longitude))

df_lignes_final = spark.createDataFrame(rows,["libelle_ligne", "type_ligne", "point_number", "latitude", "longitude"])
        
df_lignes_final.write.mode('overwrite').option("header", True).csv(f"hdfs://localhost:9000/{writing_path}/lignes.csv")
df_lignes_final.write.mode('overwrite').option("header", True).csv(f"hdfs://localhost:9000/power_bi/lignes.csv")

print(f"Transforming Objets-trouvés data")
df_objets_trouves = spark.read.parquet(f"hdfs://localhost:9000/{reading_path}/objets-trouves.parquet")
df_objets_trouves.write.mode('overwrite').option("header", True).csv(f"hdfs://localhost:9000/power_bi/objets-trouves.csv")
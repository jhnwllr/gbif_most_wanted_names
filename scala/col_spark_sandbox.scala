
if(FALSE) { // intial effort to explore data 
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val database = "prod_h"
// val database = "uat"

val df_taxonrank_mappings = spark.read.
option("sep", "\t").
option("header", "true").
option("inferSchema", "true").
csv("taxon_rank_mappings.tsv")

val df_original = sqlContext.sql("SELECT * FROM " + database + ".occurrence")

val keep_kingdoms = List("Animalia", "Plantae", "Fungi")

val df_hrank = df_original.
filter($"kingdom".isin(keep_kingdoms:_*)).
filter(array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK"))

val ignore_ranks = List("GENUS","SPECIES","FAMILY")

val df_rank_counts=df_hrank.
groupBy("v_taxonrank","taxonrank","kingdom").
agg(
count(lit(1)).alias("count"),
countDistinct("v_scientificname").as("names")
).
orderBy(desc("count"))

val interesting_concepts = List("KINGDOM","PHYLUM","CLASS","ORDER","FAMILY","GENUS","SPECIES")

df_rank_counts.join(
df_taxonrank_mappings,
df_rank_counts("v_taxonrank") === 
df_taxonrank_mappings("Value")
).
filter($"Concept".isin(interesting_concepts:_*)).
withColumnRenamed("v_taxonrank","verbatim").
withColumnRenamed("taxonrank","interpreted").
withColumnRenamed("Concept","mapped").
select("kingdom","verbatim","interpreted","mapped","count","names").
withColumn("inter_level",
when(col("interpreted") === "SPECIES", 1).
when(col("interpreted") === "GENUS", 2).
when(col("interpreted") === "FAMILY", 3).
when(col("interpreted") === "ORDER", 4).
when(col("interpreted") === "CLASS", 5).
when(col("interpreted") === "PHYLUM", 6).
when(col("interpreted") === "KINGDOM", 7).
otherwise(null)).
withColumn("inter_map",
when(col("mapped") === "SPECIES", 1).
when(col("mapped") === "GENUS", 2).
when(col("mapped") === "FAMILY", 3).
when(col("mapped") === "ORDER", 4).
when(col("mapped") === "CLASS", 5).
when(col("mapped") === "PHYLUM", 6).
when(col("mapped") === "KINGDOM", 7).
otherwise(null)).
withColumn("difference",$"inter_level" - $"inter_map").
filter($"difference" > 2). 
filter($"interpreted" === "KINGDOM").
filter($"mapped" === "SPECIES").
show(100)

}


if(FALSE) { // table printing and exploration

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val database = "prod_h"

val df_taxonrank_mappings = spark.read.
option("sep", "\t").
option("header", "true").
option("inferSchema", "true").
csv("taxon_rank_mappings.tsv")

val df_original = sqlContext.sql("SELECT * FROM " + database + ".occurrence")

val keep_kingdoms = List("Animalia", "Plantae", "Fungi")
 
val df_hrank = df_original.
filter(!$"v_scientificname".contains("BOLD")). 
filter(!$"v_scientificname".contains("gen.")). 
filter(!$"v_scientificname".contains("sp.")). 
filter(!$"v_scientificname".contains("SDJB")). 
filter(!$"v_scientificname".contains("\\ssp")). 
filter(!$"v_scientificname".contains("VOB")). 
filter($"kingdom".isin(keep_kingdoms:_*)).
filter(array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK")).join(
df_taxonrank_mappings,
df_original("v_taxonrank") === 
df_taxonrank_mappings("Value")
).
withColumnRenamed("Concept","mapped").
withColumn("inter_rank_level",
when(col("taxonrank") === "SPECIES", 1).
when(col("taxonrank") === "GENUS", 2).
when(col("taxonrank") === "FAMILY", 3).
when(col("taxonrank") === "ORDER", 4).
when(col("taxonrank") === "CLASS", 5).
when(col("taxonrank") === "PHYLUM", 6).
when(col("taxonrank") === "KINGDOM", 7).
otherwise(null)).
withColumn("map_rank_level",
when(col("mapped") === "SPECIES", 1).
when(col("mapped") === "GENUS", 2).
when(col("mapped") === "FAMILY", 3).
when(col("mapped") === "ORDER", 4).
when(col("mapped") === "CLASS", 5).
when(col("mapped") === "PHYLUM", 6).
when(col("mapped") === "KINGDOM", 7).
otherwise(null)).
withColumn("difference",$"inter_rank_level" - $"map_rank_level")

// a priori group exploration 

val df_total_counts = df_original.
groupBy("familykey").
agg(count(lit(1)).alias("total_count"))

// filter($"orderkey" === 1470).
val df_results = df_hrank.
filter($"difference" > 1).
filter($"classkey" === 180).
groupBy("order_","family","familykey","taxonrank","mapped","difference").
agg(
count(lit(1)).alias("count"),
countDistinct("v_scientificname").as("names"),
countDistinct("datasetkey").as("datasets")
).
orderBy(desc("count")).
drop("difference").
join(df_total_counts,"familykey").
withColumn("percent",round(($"count"/$"total_count")*100,0)).
orderBy(desc("count"))

// friendly table print 

df_results.
withColumnRenamed("taxonrank","gbif_rank").
withColumnRenamed("mapped","pub_rank").
withColumnRenamed("order_","order").
withColumnRenamed("count","n_occ").
withColumnRenamed("datasets","n_ds").
withColumnRenamed("total_count","n_tot").
withColumnRenamed("percent","per").
select("familykey",
"order",
"family",
"gbif_rank",
"pub_rank",
"names",
"n_occ",
"n_tot",
"per",
"n_ds"
).
show(100,20)

// family name break down 
df_hrank.
filter($"familykey" === 3292693).
groupBy("v_scientificname","mapped","taxonrank").
agg(
count(lit(1)).alias("n_occ"),
countDistinct("datasetkey").as("n_ds")
).
orderBy(desc("n_occ")).
filter($"taxonrank" === "FAMILY" && $"mapped" === "SPECIES").
show(100,50)
}

if(FALSE) { // use regex to match "normal looking" names to filter out OTUs
// \b\w{3,} \w{4,}\b

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df_taxonrank_mappings = spark.read.
option("sep", "\t").
option("header", "true").
option("inferSchema", "true").
csv("taxon_rank_mappings.tsv")

// val df_original = sqlContext.sql("SELECT * FROM prod_h.occurrence")
val df_original = sqlContext.sql("SELECT * FROM jwaller.occurrence_20210204")

val keep_kingdoms = List("Animalia", "Plantae", "Fungi")

val df_hrank = df_original.
filter(!$"basisofrecord".contains("FOSSIL_SPECIMEN")).
filter(!$"basisofrecord".contains("MATERIAL_SAMPLE")).
filter($"v_scientificname" rlike """^\b\w{4,} \w{4,}\b""").
filter(!$"v_scientificname".contains("BOLD")). 
filter(!$"v_scientificname".contains("gen.")). 
filter(!$"v_scientificname".contains("sp.")). 
filter(!$"v_scientificname".contains("SDJB")). 
filter(!$"v_scientificname".contains("\\ssp")). 
filter(!$"v_scientificname".contains("VOB")). 
filter(!$"v_scientificname".contains("indet")). 
filter(!$"v_scientificname".contains("""uncultured """)).  
filter(!$"v_scientificname".contains("""environmental sample""")).  
filter($"kingdom".isin(keep_kingdoms:_*)).
filter(array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK")).join(
df_taxonrank_mappings,
df_original("v_taxonrank") === 
df_taxonrank_mappings("Value")
).
withColumnRenamed("Concept","mapped").
withColumn("inter_rank_level",
when(col("taxonrank") === "SPECIES", 1).
when(col("taxonrank") === "GENUS", 2).
when(col("taxonrank") === "FAMILY", 3).
when(col("taxonrank") === "ORDER", 4).
when(col("taxonrank") === "CLASS", 5).
when(col("taxonrank") === "PHYLUM", 6).
when(col("taxonrank") === "KINGDOM", 7).
otherwise(null)).
withColumn("map_rank_level",
when(col("mapped") === "SPECIES", 1).
when(col("mapped") === "GENUS", 2).
when(col("mapped") === "FAMILY", 3).
when(col("mapped") === "ORDER", 4).
when(col("mapped") === "CLASS", 5).
when(col("mapped") === "PHYLUM", 6).
when(col("mapped") === "KINGDOM", 7).
otherwise(null)).
withColumn("difference",$"inter_rank_level" - $"map_rank_level")

val df_matchable_names = df_hrank.
filter($"mapped" === "SPECIES").
groupBy("kingdom","phylum","class","order_","family","kingdomkey","phylumkey","orderkey","classkey","familykey","v_scientificname","mapped","taxonrank").
agg(
count(lit(1)).alias("count"),
countDistinct("datasetkey").as("datasets")
).
orderBy(desc("count"))

// df_matchable_names.count()
// 280008
df_matchable_names.write.parquet("df_matchable_names_20210204.parquet")
// df_matchable_names.write.parquet("df_matchable_names_20210408.parquet")

// FILE_NAME="df_matchable_names_20210408"
FILE_NAME="df_matchable_names_20210204"
rm -r  /mnt/auto/misc/download.gbif.org/custom_download/jwaller/$FILE_NAME".parquet.zip"
hadoop fs -get /user/jwaller/$FILE_NAME".parquet"  /mnt/auto/misc/download.gbif.org/custom_download/jwaller/$FILE_NAME".parquet"
hdfs dfs -rm -r $FILE_NAME".parquet"
cd /mnt/auto/misc/download.gbif.org/custom_download/jwaller/
zip -r $FILE_NAME".parquet.zip" $FILE_NAME".parquet"
rm -r  /mnt/auto/misc/download.gbif.org/custom_download/jwaller/$FILE_NAME".parquet"
exit

// FILE_NAME="df_matchable_names_20210408"
FILE_NAME="df_matchable_names_20210204"
DATA_DIR="/cygdrive/c/users/ftw712/desktop/col_gaps/data"
cd $DATA_DIR
wget "http://download.gbif.org/custom_download/jwaller/"$FILE_NAME".parquet.zip"
unzip $FILE_NAME".parquet.zip"
rm $FILE_NAME".parquet.zip"
ls -lh

}

// Get a table of very likely to be matchable names 


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df_taxonrank_mappings = spark.read.
option("sep", "\t").
option("header", "true").
option("inferSchema", "true").
csv("taxon_rank_mappings.tsv")

val df_original = sqlContext.sql("SELECT * FROM prod_h.occurrence")

val df_old_backbone = sqlContext.sql("SELECT * FROM jwaller.occurrence_20210204").
withColumn("is_hrank_old",array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK")).
withColumnRenamed("taxonrank","taxonrank_old").
select("is_hrank_old","taxonrank_old","v_scientificname").

val keep_kingdoms = List("Animalia", "Plantae", "Fungi")

val df_filtered = df_original.
filter(!$"basisofrecord".contains("FOSSIL_SPECIMEN")).
filter(!$"basisofrecord".contains("MATERIAL_SAMPLE")).
filter($"v_scientificname" rlike """^\b\w{4,} \w{4,}\b""").
filter(!$"v_scientificname".contains("BOLD")). 
filter(!$"v_scientificname".contains("gen.")). 
filter(!$"v_scientificname".contains("sp.")). 
filter(!$"v_scientificname".contains("SDJB")). 
filter(!$"v_scientificname".contains("\\ssp")). 
filter(!$"v_scientificname".contains("VOB")). 
filter(!$"v_scientificname".contains("indet")). 
filter(!$"v_scientificname".contains("""uncultured """)).  
filter(!$"v_scientificname".contains("""environmental sample""")).  
filter($"kingdom".isin(keep_kingdoms:_*))

val df = df_filtered.
withColumn("is_hrank_new",array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK")).join(
df_taxonrank_mappings,
df_original("v_taxonrank") === 
df_taxonrank_mappings("Value")
).
withColumnRenamed("Concept","mapped")

df_old_backbone.
groupBy("is_hrank_old","taxonrank_old","v_scientificname").
agg(
count(lit(1)).alias("count")
)


val df_matchable_names = df.
filter($"mapped" === "SPECIES").
groupBy("kingdom","phylum","class","order_","family","kingdomkey","phylumkey","orderkey","classkey","familykey","v_scientificname","mapped","taxonrank","is_hrank_new").
agg(
count(lit(1)).alias("count"),
countDistinct("datasetkey").as("datasets")
).
orderBy(desc("count"))


// 2 179 350



// most wanted v_scientificname names

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val df_taxonrank_mappings = spark.read.
option("sep", "\t").
option("header", "true").
option("inferSchema", "true").
csv("taxon_rank_mappings.tsv")

val df_original = sqlContext.sql("SELECT * FROM prod_h.occurrence")

val keep_kingdoms = List("Animalia", "Plantae", "Fungi")
val exclude_basis_of_record = List("FOSSIL_SPECIMEN","MATERIAL_SAMPLE")

val black_list = List(
"BOLD",
"gen.",
"Mystery mystery",
"Sonus naturalis",
"BOLD",
"gen.",
"sp.",
"SDJB",
"\\ssp",
"VOB",
"indet",
"uncultured ",  
"environmental sample",
"INCERTAE",
"Unplaced",
"Unknown"
)

val df_filtered = df_original.
filter(array_contains(col("issue"), "TAXON_MATCH_HIGHERRANK")).
filter($"v_scientificname" rlike """^\b\w{4,} \w{4,}\b""").
filter($"kingdom".isin(keep_kingdoms:_*)).
filter(!$"basisofrecord".isin(exclude_basis_of_record:_*)).
filter(!col("v_scientificname").rlike(black_list.mkString("|")))

val df = df_filtered.
join(
df_taxonrank_mappings,
df_original("v_taxonrank") === 
df_taxonrank_mappings("Value")
).
withColumnRenamed("Concept","publisher_rank")

val df_matchable_names = df.
filter($"publisher_rank" === "SPECIES").
filter(!$"taxonrank".contains("GENUS")).
filter(!$"taxonrank".contains("SPECIES")).
groupBy("kingdom","phylum","class","order_","family","v_kingdom","v_phylum","v_class","v_order","v_family","v_genus","kingdomkey","phylumkey","orderkey","classkey","familykey","scientificname","v_scientificname","publisher_rank","taxonrank").
agg(
count(lit(1)).alias("occ_count"),
countDistinct("datasetkey").as("n_dataset"),
countDistinct("publishingorgkey").as("n_publisher"),
collect_set("datasetkey").as("datasetkeys_array")
).
orderBy(desc("occ_count")).
filter($"occ_count" > 10).
withColumn("datasetkeys",concat_ws(";", $"datasetkeys_array")).
drop("datasetkeys_array").
withColumnRenamed("scientificname","interpreted_scientificname")

// Run through name parser API

val df_request = df_matchable_names.select("v_scientificname")

import sys.process._
import org.apache.spark.sql.SaveMode

df_request.
write.format("csv").
option("sep", "\t").
option("header", "false").
mode(SaveMode.Overwrite).
save("request")

(s"rm request.txt")!
(s"rm response.json")!
(s"hdfs dfs -getmerge /user/jwaller/request request.txt")!
Seq("curl","-X","POST","-F","names=@request.txt","https://api.gbif.org/v1/parser/name","-o","response.json").!
(s"head response.json")!
(s"hdfs dfs -put -f /home/jwaller/response.json response.json")!
(s"hdfs dfs -ls")!

val df_response = spark.read.json("response.json")

val df_export = df_matchable_names.join(
df_response,
df_response("scientificName") === 
df_matchable_names("v_scientificname")
).
drop("scientificName").
filter(
($"type" === "SCIENTIFIC")
or
($"type" === "INFORMAL")
).
filter($"v_scientificname".isNotNull)

// save most wanted names
val save_table_name = "gbif_most_wanted_names"

df_export.
write.format("csv").
option("sep", "\t").
option("header", "false"). // add header later
mode(SaveMode.Overwrite).
save(save_table_name)

// custom downloads dir 
val export_dir = "/mnt/auto/misc/download.gbif.org/custom_download/jwaller/"

// export tsv file from scala to custom downloads 
(s"hdfs dfs -ls")!
(s"rm " + export_dir + save_table_name)!
(s"hdfs dfs -getmerge /user/jwaller/"+ save_table_name + " " + export_dir + save_table_name+ ".tsv")!
(s"head " + export_dir + save_table_name + ".tsv")!
val header = "1i " + df_export.columns.toSeq.mkString("""\t""")
Seq("sed","-i",header,export_dir+save_table_name+".tsv").!

// The the table give you the most wanted strings that are likely not garbage to be matches
// yarn application -kill application_1607538765634_363875


// (s"ls -lh " + export_dir)!
// (s"rm -r " + export_dir + save_table_name + ".zip")!
// (s"zip -j " + export_dir + save_table_name + ".zip" + " " + export_dir + save_table_name +".tsv")! // no junk paths
// (s"ls -lh " + export_dir)!
// (s"rm -r " + export_dir + save_table_name +".tsv")!


// curl -X POST -H "Content-Type: text/plain" --data "@request.txt" https://api.gbif.org/v1/parser/name?name= > output.json




// df_matchable_names.show()


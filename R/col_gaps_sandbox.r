
if(FALSE) { # 
library(dplyr)
data_dir = "C:/Users/ftw712/Desktop/col_gaps/data/"

readr::read_tsv(paste0(data_dir,"taxon_rank_mappings.tsv")) 


# FILE_NAME="taxon_rank_mappings.tsv" 
# COPY_DIR="/cygdrive/c/Users/ftw712/Desktop/col_gaps/data/"
# scp -r $COPY_DIR$FILE_NAME jwaller@c5gateway-vh.gbif.org:/home/jwaller/

# hdfs dfs -put "taxon_rank_mappings.tsv"
}

if(FALSE) { # clean up spark table
library(dplyr)
library(purrr)
library(magrittr)
library(stringr)

readLines("C:/Users/ftw712/Desktop/col_gaps/data/data_table.txt") %>%
extract(-1) %>% 
extract(-2) %>%
map(~ stringr::str_replace_all(.x,"  ","")) %>%
map(~ stringr::str_replace_all(.x,"[\\|] ","|")) %>%
map(~ stringr::str_replace_all(.x,"[\\|]","\t")) %>% 
flatten_chr() %>% 
writeLines("C:/Users/ftw712/Desktop/col_gaps/data/data_table_outpute.txt")

}

if(FALSE) { 
library(dplyr)
library(purrr)

data_dir = "C:/Users/ftw712/Desktop/col_gaps/data/"
# list.files(data_dir)
old_backbone = parqr::parquet_readr(paste0(data_dir,"df_matchable_names_20210204.parquet")) 

new_backbone = parqr::parquet_readr(paste0(data_dir,"df_matchable_names_20210408.parquet"))

new_unmatched = new_backbone %>% pull(v_scientificname)

improved_names = old_backbone %>%
glimpse() %>% 
mutate(in_new = v_scientificname %in% new_unmatched) %>%
filter(!in_new) %>% 
glimpse()


improved_names %>%
group_by(class) %>% 
summarise(occ_count=sum(count),names=n()) %>%
arrange(-names) 

}

if(FALSE) { # run most wanted names through name parser
}

if(FALSE) { 
library(dplyr) 
data.table::fread("C:/Users/ftw712/Desktop/col_gaps/data/gbif_most_wanted_names.tsv") %>% 
glimpse() %>% 
select(v_scientificname) %>% 
readr::write_tsv("C:/Users/ftw712/Desktop/col_gaps/data/request.txt")

url = "https://api.gbif.org/v1/parser/name?name="

library(httr)
library(dplyr) 

POST(url = url, 
add_headers("Content-Type: plain/text content"),
body = upload_file("C:/Users/ftw712/Desktop/col_gaps/data/request.txt"),
encode = 'json') %>% 
content(as = "text") %>% 
jsonlite::fromJSON()
 
# curl --header "Content-Type: plain/text content" --data "fileupload=@request.txt" https://api.gbif.org/v1/parser/name?name=


# curl -X POST -H "Content-Type: text/plain" --data "@request.txt" https://api.gbif.org/v1/parser/name?name= > output.txt

 
 # %>% 
# saveRDS("C:/Users/ftw712/Desktop/col_gaps/data/parsed_names.rda")

# curl -X POST -F 'request.txt' 'https://api.gbif.org/v1/parser/name?name='
# curl --data @request.txt https://api.gbif.org/v1/parser/name?name=
}

if(FALSE) {
library(dplyr)

d = readRDS("C:/Users/ftw712/Desktop/col_gaps/data/parsed_names.rda") %>%
glimpse() 


d %>%
group_by(type) %>%
count()


d %>% 
filter(type == "INFORMAL") %>%
select(scientificName) %>%
readr::write_tsv("C:/Users/ftw712/Desktop/doubtful.tsv")
}


library(dplyr)


readr::read_tsv("http://download.gbif.org/custom_download/jwaller/gbif_most_wanted_names.tsv") %>% 
select(
kingdom,
v_scientificname,
publisher_rank,
taxonrank,
occ_count,
n_dataset,
n_publisher
) %>%
glimpse() %>%
head(10) %>%
readr::write_csv("C:/Users/ftw712/Desktop/top_ten_names.csv")


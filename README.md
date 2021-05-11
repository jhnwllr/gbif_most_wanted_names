## GBIF most wanted names

What is a "most wanted name"? It is a **species-rank name that does not match to the GBIF backbone**. A names in this context is any "reasonable looking" string of characters. Since un-interpreted free text from data publishers can vary significantly, **it is necessary filter these verbatim names to get back something worth pursing**. 

A "reasonable looking" binomial name has the following properties according to the most wanted names script: 

* Has **two words** of at least 4 characters each separated by a space. 
* Is **not a OTU** (Operational taxonomic unit) 
* Is **not a fossil** (it might be reasonable to not match about Family for a fossil. 
* Matches **above Genus level**
* Is labeled as **rank SPECIES** by the publisher 
* **Passes through the GBIF name parser** as either `type` : "SCIENTIFIC" or "INFORMAL"
* More than 10 occurrence records tied to the names 
* Is flagged as TAXON_MATCH_HIGHERRANK

Importantly this script might miss wanted names that do not fill in the  `v_taxonrank` field. This is probably pretty common.

This repository generates a table of most wanted names and exports them to custom downloads. 

## Top 10 most wanted names 

| kingdom  | v_scientificname                                            | publisher_rank | taxonrank | occ_count | n_dataset | n_publisher |
| -------- | ----------------------------------------------------------- | -------------- | --------- | --------- | --------- | ----------- |
| Plantae  | Rhynchelytrium nepeng                                       | SPECIES        | FAMILY    | 4562      | 2         | 1           |
| Plantae  | Aliaria petiolata (M.Bieb.) Cavara & Grande                 | SPECIES        | FAMILY    | 1579      | 3         | 1           |
| Plantae  | Iris xiphium (L.) Dryand. ex Ait.                           | SPECIES        | FAMILY    | 1202      | 2         | 1           |
| Fungi    | Candolleomyces candolleanus                                 | SPECIES        | FAMILY    | 1150      | 1         | 1           |
| Animalia | Expapillata firmatoi (Barretto, Martins & Pellegrino, 1956) | SPECIES        | FAMILY    | 1024      | 1         | 1           |
| Plantae  | Callophycus laxus                                           | SPECIES        | FAMILY    | 828       | 5         | 5           |
| Animalia | Tropicagama temporalis                                      | SPECIES        | FAMILY    | 686       | 3         | 3           |
| Fungi    | Sterile sorediate crust                                     | SPECIES        | FAMILY    | 604       | 1         | 1           |
| Animalia | Ganglionus catenatus                                        | SPECIES        | FAMILY    | 482       | 2         | 2           |
| Animalia | Deanemyia samueli (Deane, 1955)                             | SPECIES        | FAMILY    | 460       | 1         | 1           |

find the full dataset [here](http://download.gbif.org/custom_download/jwaller/gbif_most_wanted_names.tsv).

## How to run

This is simply a scala-spark script. It could be developed further into something that runs periodically or after each backbone build to compare wanted names. 

Setup 

```shell



```

Run matchable names 

You can 

```scala 


```












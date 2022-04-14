# VFB_json_schema_indexer

This repository provides a Solr based caching solution to the [VFB_json_schema](https://github.com/VirtualFlyBrain/VFB_json_schema).

## Build

To build and run the indexer, execute the following commands in the project root folder. 

```
docker build -t virtualflybrain/vfb_json_indexer .

docker run --volume=/my/output/folder:/output/ -e PDBuser=myuser -e PDBpassword=mypass -e OutputPath=/output/solr_index.json -it virtualflybrain/vfb_json_indexer
```
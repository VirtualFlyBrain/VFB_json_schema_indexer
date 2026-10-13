# VFB_json_schema_indexer

This repository provides a Solr based caching solution to the [VFB_json_schema](https://github.com/VirtualFlyBrain/VFB_json_schema).

## Build

To build and run the indexer, execute the following commands in the project root folder. 

```
docker build -t virtualflybrain/vfb_json_indexer .

docker run --volume=/my/output/folder:/output/ -e PDBuser=myuser -e PDBpassword=mypass -e OutputPath=/output/solr_index.json -it virtualflybrain/vfb_json_indexer
```
## Options (environment)

| Variable | Default | Meaning |
|---|---|---|
| `EXPORT_MODE` | `file` | `stream`: APOC streams each batch's export back over HTTP instead of writing into the Neo4j import dir (runners with no filesystem shared with Neo4j). |
| `EXPORT_STREAM_TIMEOUT` | `7200` | `stream` mode only: seconds APOC waits for one batch before failing. Raise it for slow single terms (e.g. a large template's ROI/dataset tree on a busy server). |
| `CHANGED_IDS_FILE` | unset | Append `<service>\t<id>` for every document written with a value different from the one already in Solr, plus every new one. Lets a release refresh downstream caches (VFBquery, v3-cached) for just those terms. |
| `SKIP_UNCHANGED` | `false` | `true`: do not rewrite documents whose value is unchanged (less Solr churn on a stale refresh). |

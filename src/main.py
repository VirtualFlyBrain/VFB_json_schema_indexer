import os
import json
import logging
import requests
from typing import Dict
from src.indexers.anat_image_query_indexer import AnatImageQueryIndexer
from src.indexers.anat_query_indexer import AnatQueryIndexer
from src.indexers.anat_2_ep_query_indexer import Anat2EpQueryIndexer
from src.indexers.ep_2_anat_query_indexer import Ep2AnatQueryIndexer
from src.indexers.template_2_datasets_query_indexer import Template2DatasetsQueryIndexer
from src.indexers.all_datasets_query_indexer import AllDatasetsQueryIndexer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BATCH_FILE_LOCATION = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../indexes/solr_index.json")


def main() -> None:
    """
    Generates solr indexes for all registered indexers and merges them to generate a unified solr index. Saves unified
    index to a file defined by the 'OutputPath' environment variable.
    """
    indexers = [AnatImageQueryIndexer(), AnatQueryIndexer(), Anat2EpQueryIndexer(), Ep2AnatQueryIndexer(),
                Template2DatasetsQueryIndexer(), AllDatasetsQueryIndexer()]

    all_data = dict()
    for indexer in indexers:
        service_data = indexer.generate_index()
        merge_to_main_index(all_data, service_data)

    dump_dict_to_file(all_data, os.getenv('OutputPath', BATCH_FILE_LOCATION))


def merge_to_main_index(all_data: Dict[str, Dict], service_data: Dict[str, Dict]) -> None:
    """
    Merges service generated index to the main index.
    :param all_data: main index dictionary
    :param service_data: service index dictionary
    """
    for solr_id in service_data:
        if solr_id in all_data:
            solr_doc = service_data[solr_id]
            for solr_doc_key in solr_doc:
                if solr_doc_key != "id":
                    all_data[solr_id][solr_doc_key] = solr_doc[solr_doc_key]
        else:
            all_data[solr_id] = service_data[solr_id]


def dump_dict_to_file(dict_data: Dict[str, Dict], path: str) -> None:
    """
    Dumps values of the dictionary to the file as list of values
    :param dict_data: dictionary of entities
    :param path: output file path
    """
    log.info("Writing data to file. Object count is : " + str(len(dict_data.values())))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(list(dict_data.values()), f, ensure_ascii=False, indent=4)


def update_solr(all_data: Dict[str, Dict]) -> None:
    """
    Pushes solr_data to the Solr server with an update request. This function expects Solr collections specified by the
    'SOLRcollection' already exists in the Solr server.
    :param all_data: solr index dictionary
    """
    server = os.environ["SOLRserver"]
    collection = os.environ["SOLRcollection"]
    if not server.endswith("/"):
        server += "/"
    url = server + collection + "/update"
    # url = "http://localhost:8993/solr/vfb_json/update"

    log.info("Sending data to solr: " + url)
    headers = {"Content-type": "application/json"}
    params = {"commit": "true"}
    r = requests.post(url, data=json.dumps(all_data.values()), params=params, headers=headers)

    if r.status_code != 200:
        log.error("Solr indexing failed (%s): %s" % (r.status_code, r.text))
    else:
        log.info("Solr indexing is SUCCESSFUL")


if __name__ == '__main__':
    # TODO delete environment variables on deployment
    # os.environ["PDBserver"] = "http://pdb.v4.virtualflybrain.org"
    # os.environ["PDBuser"] = "user"
    # os.environ["PDBpassword"] = "password"

    # os.environ["OutputPath"] = BATCH_FILE_LOCATION
    # os.environ["SOLRserver"] = "http://localhost:8993/solr"
    # os.environ["SOLRcollection"] = "vfb_json"

    main()



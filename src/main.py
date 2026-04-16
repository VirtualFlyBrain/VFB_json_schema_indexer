import os
import json
import logging
import time
from typing import Dict
import concurrent.futures

from src.indexers.anat_image_query_indexer import AnatImageQueryIndexer
from src.indexers.anat_query_indexer import AnatQueryIndexer
from src.indexers.anat_2_ep_query_indexer import Anat2EpQueryIndexer
from src.indexers.ep_2_anat_query_indexer import Ep2AnatQueryIndexer
from src.indexers.template_2_datasets_query_indexer import Template2DatasetsQueryIndexer
from src.indexers.all_datasets_query_indexer import AllDatasetsQueryIndexer
from src.indexers.term_info.license_term_info_indexer import LicenseTermInfoQueryIndexer
from src.indexers.term_info.anatomical_ind_term_info_indexer import AnatomicalIndTermInfoQueryIndexer
from src.indexers.term_info.class_term_info_indexer import ClassTermInfoQueryIndexer
from src.indexers.term_info.neuron_class_term_info_indexer import NeuronClassTermInfoQueryIndexer
from src.indexers.term_info.split_class_term_info_indexer import SplitClassTermInfoQueryIndexer
from src.indexers.term_info.dataset_term_info_indexer import DatasetTermInfoQueryIndexer
from src.indexers.term_info.pub_term_info_indexer import PubTermInfoQueryIndexer
from src.indexers.term_info.template_term_info_indexer import TemplateTermInfoQueryIndexer
from src.indexers.term_info.cluster_term_info_indexer import ClusterTermInfoQueryIndexer
from src.indexers.scRNAseq.anat_scRNAseq_query_indexer import AnatScRNASeqQueryIndexer
from src.indexers.scRNAseq.cluster_expression_query_indexer import ClusterExpressionQueryIndexer
from src.indexers.connectivity.neuron_downstream_connectivity_indexer import NeuronDownstreamConnectivityIndexer
from src.indexers.connectivity.neuron_upstream_connectivity_indexer import NeuronUpstreamConnectivityIndexer
from src.solr_client import send_solr_docs, send_solr_payload, send_final_commit
from src.progress import ProgressTracker
from tqdm import tqdm

COMBINED_INDEX_CHUNK = int(os.getenv("COMBINED_INDEX_CHUNK", 5000))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BATCH_FILE_LOCATION = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../indexes/solr_index.json")


def main() -> None:
    """
    Generates solr indexes for all registered indexers and merges them to generate a unified solr index. Saves unified
    index to a file defined by the 'OutputPath' environment variable.
    """
    # Order term_info_indexers to prioritize anatomical individuals before class term_info.
    term_info_indexers = [
        AnatomicalIndTermInfoQueryIndexer(),  # Individual
        ClassTermInfoQueryIndexer(),          # Class (base)
        NeuronClassTermInfoQueryIndexer(),    # Neuron Class
        SplitClassTermInfoQueryIndexer(),     # Split Class
        ClusterTermInfoQueryIndexer(),        # Cluster
        TemplateTermInfoQueryIndexer(),       # Template
        PubTermInfoQueryIndexer(),            # Publication
        DatasetTermInfoQueryIndexer(),        # DataSet
        LicenseTermInfoQueryIndexer()         # License
    ]

    # Define other indexers
    other_indexers = [
        AnatImageQueryIndexer(),
        AnatQueryIndexer(),
        Anat2EpQueryIndexer(),
        Ep2AnatQueryIndexer(),
        Template2DatasetsQueryIndexer(),
        AllDatasetsQueryIndexer(),
        AnatScRNASeqQueryIndexer(),
        ClusterExpressionQueryIndexer(),
        NeuronDownstreamConnectivityIndexer(),
        NeuronUpstreamConnectivityIndexer()
    ]

    # Combine the indexers
    indexers = term_info_indexers + other_indexers

    all_data = dict()

    # Progress tracker — persists state to $WORKSPACE/indexer_progress.json (or
    # $PROGRESS_FILE) so a restarted job skips (phase, indexer) pairs that
    # already completed. The pair that was in flight when the previous run
    # died is reset to pending and re-runs; partition_ids() will re-probe Solr
    # and naturally skip ids already written.
    total_pairs = 2 * len(indexers)
    tracker = ProgressTracker()
    tracker.load()
    tracker.set_total_pairs(total_pairs)
    run_start = time.time()

    # Create a ThreadPoolExecutor for Solr updates
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_indexer = {}

        # Two-phase global run: first fill every indexer's MISSING docs, only
        # then refresh STALE ones. If the job dies midway the site already has
        # first-time coverage across every service, rather than full coverage
        # for the first few services and nothing for the rest.
        ordinal = 0
        for phase in ("missing", "stale"):
            log.info(f"=== Starting phase '{phase}' for all indexers ===")
            for indexer in indexers:
                ordinal += 1
                indexer_name = type(indexer).__name__

                if tracker.should_skip(phase, indexer_name):
                    tracker.log_skip(phase, indexer_name, ordinal, total_pairs)
                    continue

                tracker.mark_in_progress(phase, indexer_name, ordinal, total_pairs)
                t0 = time.time()
                try:
                    service_data = indexer.generate_index(phase=phase)
                except Exception as exc:
                    tracker.mark_failed(
                        phase, indexer_name, ordinal, total_pairs, exc,
                        duration_seconds=time.time() - t0,
                    )
                    log.error('%r (phase=%s) crawl generated an exception: %s' % (indexer, phase, exc))
                    raise

                if not service_data:
                    tracker.mark_empty(
                        phase, indexer_name, ordinal, total_pairs,
                        duration_seconds=time.time() - t0,
                    )
                    continue

                prepare_for_atomic_update(service_data)
                # Submit the Solr update task to the executor. We mark the pair
                # "completed" only after this future resolves — the crawl alone
                # isn't a success if the combined_index write fails.
                future = executor.submit(update_solr_with_data, service_data)
                future_to_indexer[future] = (indexer, indexer_name, phase, ordinal, t0)
                # Proceed to the next indexer without waiting for Solr update
                merge_to_main_index(all_data, service_data)

        # Optionally, dump all data to a file
        dump_dict_to_file(all_data, os.getenv('OutputPath', BATCH_FILE_LOCATION))

        # Wait for all Solr updates to complete and handle exceptions
        for future in concurrent.futures.as_completed(future_to_indexer):
            indexer, indexer_name, phase, ordinal, t0 = future_to_indexer[future]
            duration = time.time() - t0
            try:
                future.result()
            except Exception as exc:
                tracker.mark_failed(
                    phase, indexer_name, ordinal, total_pairs, exc,
                    duration_seconds=duration,
                )
                log.error('%r (phase=%s) generated an exception: %s' % (indexer, phase, exc))
            else:
                tracker.mark_completed(
                    phase, indexer_name, ordinal, total_pairs,
                    duration_seconds=duration,
                )

    tracker.finalize(total_elapsed_seconds=time.time() - run_start)


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

def prepare_for_atomic_update(service_data: Dict[str, Dict]) -> None:
    for doc_id, doc_fields in service_data.items():
        for field_name, field_value in list(doc_fields.items()):
            # Skip the id field
            if field_name == 'id':
                continue
                
            # Check if the value is already formatted for atomic update
            if isinstance(field_value, dict) and len(field_value) == 1 and next(iter(field_value.keys())) in ('set', 'add', 'inc', 'remove'):
                # Already in atomic update format, leave it as is
                continue
                
            # Apply atomic update formatting
            doc_fields[field_name] = {'set': field_value}

def update_solr_with_data(solr_data: Dict[str, Dict]) -> None:
    """Chunk the combined_index write. Previously this shipped ~749k docs in a
    single POST which repeatedly timed out; chunking keeps each request well
    under SOLR_WRITE_TIMEOUT_SECONDS and plays nicely with commitWithin.
    """
    docs = list(solr_data.values())
    total = len(docs)
    if total == 0:
        log.info("combined_index: no documents to index.")
        return

    for start in tqdm(
        range(0, total, COMBINED_INDEX_CHUNK),
        total=(total + COMBINED_INDEX_CHUNK - 1) // COMBINED_INDEX_CHUNK,
        desc="combined_index",
    ):
        send_solr_docs(
            docs[start:start + COMBINED_INDEX_CHUNK],
            service_name="combined_index",
        )

    send_final_commit("combined_index")


def update_solr(payload, service_name: str = "combined_index") -> None:
    """
    Pushes payload to the Solr server with an update request.
    This function expects Solr collections specified by the
    Pushes payload to the Solr server with an update request.
    'SOLRcollection' already exists in the Solr server.
    :param payload: solr index payload
    """
    send_solr_payload(payload, service_name=service_name)


if __name__ == '__main__':
    # TODO delete environment variables on deployment
    # os.environ["PDBserver"] = "http://pdb-dev.virtualflybrain.org"
    # os.environ["PDBuser"] = "user"
    # os.environ["PDBpassword"] = "password"
    #
    # os.environ["OutputPath"] = BATCH_FILE_LOCATION
    # os.environ["SOLRserver"] = "http://localhost:8983/solr"
    # os.environ["SOLRcollection"] = "vfb_json"

    main()
    # update_solr_from_file(BATCH_FILE_LOCATION)

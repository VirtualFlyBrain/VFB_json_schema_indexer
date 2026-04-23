import os
import math
import json
import logging
import datetime
import time
from typing import Iterable, List, Dict, Generator, Optional, Set, Tuple
from abc import ABC, abstractmethod

import requests
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor
from src.vfb.vfb_query_builder.query_roller import QueryLibrary
from src.solr_client import send_solr_docs, send_final_commit, fetch_ids_with_field
from tqdm import tqdm
from socket import error as SocketError

log = logging.getLogger(__name__)


NEO4J_MAX_RETRY_WAIT_SECONDS = int(os.getenv("NEO4J_MAX_RETRY_WAIT_SECONDS", 1200))  # 20 min
NEO4J_RETRY_INITIAL_DELAY_SECONDS = int(os.getenv("NEO4J_RETRY_INITIAL_DELAY_SECONDS", 5))
NEO4J_RETRY_MAX_DELAY_SECONDS = int(os.getenv("NEO4J_RETRY_MAX_DELAY_SECONDS", 120))
NEO4J_HEALTH_CHECK_INTERVAL = 30  # seconds between pings

CHECKPOINT_DIR_ENV = "CHECKPOINT_DIR"
CHECKPOINT_SUBDIR = "indexer_checkpoints"


def _resolve_checkpoint_dir() -> str:
    """Pick a directory for per-(phase, indexer) completed-id checkpoints.

    Resolution order mirrors src/progress.py so all resume artefacts live
    alongside each other: explicit env override, then the directory of
    ``$PROGRESS_FILE``, then ``$WORKSPACE``, then CWD.
    """
    explicit = os.environ.get(CHECKPOINT_DIR_ENV)
    if explicit:
        return explicit
    progress_file = os.environ.get("PROGRESS_FILE")
    if progress_file:
        return os.path.join(
            os.path.dirname(os.path.abspath(progress_file)), CHECKPOINT_SUBDIR
        )
    workspace = os.environ.get("WORKSPACE")
    if workspace:
        return os.path.join(workspace, CHECKPOINT_SUBDIR)
    return os.path.join(os.getcwd(), CHECKPOINT_SUBDIR)


def _checkpoint_path_for(phase: str, indexer_name: str) -> str:
    safe_phase = phase.replace(os.sep, "_")
    safe_name = indexer_name.replace(os.sep, "_")
    return os.path.join(_resolve_checkpoint_dir(), f"{safe_phase}__{safe_name}.ids")


def _load_checkpoint(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except OSError as e:
        log.warning(f"Checkpoint file unreadable at {path}: {e}. Starting fresh.")
        return set()


def _append_checkpoint(path: str, ids: Iterable[str]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            for doc_id in ids:
                if doc_id:
                    f.write(f"{doc_id}\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        log.warning(f"Could not append to checkpoint file {path}: {e}")


def _clear_checkpoint(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError as e:
        log.warning(f"Could not remove checkpoint file {path}: {e}")


class BaseQueryIndexer(ABC):
    """
    Base query class that provides an abstraction for the concrete service crawlers.
    Crawls a VFB_json_schema service with all possible parameters and generates related Solr indexes.
    """

    # 2000 ids/chunk was overwhelming Neo4j (apoc.export.json OOMing on the
    # term_info query) and producing 13–20 MB Solr payloads that tripped the
    # "Large payload" warning every single batch. 500 keeps each chunk at
    # ~4 MB, gives apoc.export a 4× smaller working set, and still amortises
    # HTTP overhead across plenty of ids per request. A pathological single
    # id that still OOMs at this size is now handled by the split-in-half
    # retry logic in _process_chunk rather than bricking the whole crawl.
    # Overridable via BATCH_SIZE env var for ad-hoc tuning.
    REQUEST_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 500))
    def __init__(self) -> None:
        self.ql = QueryLibrary()
        self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])
        # Shared Session so the HAProxy `sticky=<backend>` cookie persists
        # across retries — once a request lands on a healthy pdb.ug backend
        # the LB keeps us pinned there instead of round-robining every call.
        # On failure we clear cookies (see _drop_backend_affinity) so the LB
        # reassigns us away from a sick pod.
        self._session = requests.Session()
        self._id_partition: Optional[Tuple[List[str], List[str]]] = None

    def _drop_backend_affinity(self) -> None:
        """Forget the HAProxy sticky cookie so the next request is re-routed.
        Called whenever a request fails: the pod we were pinned to is likely
        unhealthy, so the next retry should pick a different backend."""
        self._session.cookies.clear()

    def partition_ids(self) -> Tuple[List[str], List[str]]:
        """Partition this indexer's parameter ids into (missing, stale) by
        probing Solr once. Cached on the instance so repeat calls (phase 1 and
        phase 2 of the global run) don't re-probe — and so phase 2 still sees
        the ORIGINAL stale set even though phase 1 has since populated those
        ids.
        """
        if self._id_partition is None:
            ids = self.get_query_parameters()
            service_name = self.get_service_name()
            existing = fetch_ids_with_field(ids, service_name)
            missing = [i for i in ids if i not in existing]
            stale = [i for i in ids if i in existing]
            log.info(
                f"{service_name}: {len(missing)} missing, {len(stale)} existing."
            )
            self._id_partition = (missing, stale)
        return self._id_partition

    def generate_index(self, phase: str = "all") -> Dict[str, Dict]:
        """
        Crawls VFB_json_schema API and generates Solr index data for the
        requested phase.

        :param phase: "missing" (docs absent from Solr), "stale" (docs present
            in Solr, refreshing them), or "all" (every id, no probing).
        :return: Dictionary of Solr data. Short_form as key, Solr data as value
        """
        if phase == "all":
            ids = self.get_query_parameters()
        else:
            missing, stale = self.partition_ids()
            if phase == "missing":
                ids = missing
            elif phase == "stale":
                ids = stale
            else:
                raise ValueError(f"Unknown phase: {phase}")

        if not ids:
            log.info(
                f"{self.get_service_name()}: nothing to do for phase '{phase}'."
            )
            return {}

        return self.crawl_vfb_json_data(ids, phase=phase)

    def crawl_vfb_json_data(self, ids: List[str], phase: str = "all") -> Dict[str, Dict]:
        service_name = self.get_service_name()
        indexer_name = self.__class__.__name__

        # Sort ids so chunking is deterministic across runs — essential for the
        # checkpoint-based resume below, and makes the "Exported data For X to Y"
        # log lines a readable monotonic range.
        ids = sorted(ids)

        # Per-(phase, indexer) checkpoint: every successfully written batch's ids
        # are appended to a file so a restarted run skips them instead of
        # re-crawling. `partition_ids()` already handles this naturally for the
        # missing phase (docs written mid-crash now show as existing), but the
        # stale phase has no such signal — without this checkpoint a crash at
        # batch 199/316 means the next run re-does batches 0..198, hits the same
        # memory/timeout wall, and never makes forward progress.
        checkpoint_path = _checkpoint_path_for(phase, indexer_name)
        # Log the resolved path on every run so operators can see *where*
        # checkpoints are being written — crucial on Jenkins, where the
        # default under $WORKSPACE gets wiped between builds. Set
        # $CHECKPOINT_DIR to somewhere persistent (e.g. /var/jenkins_home/
        # indexer_checkpoints) to actually get resume-across-runs.
        log.info(f"{service_name}: checkpoint path = {checkpoint_path}")
        completed_ids = _load_checkpoint(checkpoint_path)
        if completed_ids:
            original_count = len(ids)
            ids = [i for i in ids if i not in completed_ids]
            log.info(
                f"{service_name}: resuming from checkpoint {checkpoint_path} — "
                f"skipping {original_count - len(ids)} already-processed ids, "
                f"{len(ids)} remaining."
            )

        start_time = datetime.datetime.now()
        batch_size = self.REQUEST_BATCH_SIZE
        log.info(f"Crawling: '{service_name}' ({indexer_name}), "
                 f"Batch size: {batch_size}, ids: {len(ids)}, Start time: {start_time}")

        if not ids:
            log.info(f"{service_name}: nothing to crawl after checkpoint filter.")
            _clear_checkpoint(checkpoint_path)
            return {}

        chunks = get_chunks(ids, batch_size)
        vfb_json_query_template = self.get_vfb_json_query(['$ID'])

        neo4j_import_dir = '/import'  # Neo4j server import directory
        jenkins_import_dir = '/PDBupgrade/import'  # Jenkins accessible directory

        all_data = {}  # Initialize the dictionary to collect Solr documents

        vfb_json_query = vfb_json_query_template.replace("['$ID']", "$ids")

        for i, chunk in enumerate(tqdm(chunks, total=int(math.ceil(len(ids) / batch_size)), desc=self.get_service_name())):
            self._process_chunk(
                chunk=chunk,
                label=str(i),
                vfb_json_query=vfb_json_query,
                neo4j_import_dir=neo4j_import_dir,
                jenkins_import_dir=jenkins_import_dir,
                checkpoint_path=checkpoint_path,
                all_data=all_data,
            )

        # Issue a single hard commit for this service now that all batches have
        # been POSTed. Individual batches use commitWithin (see solr_client.py)
        # so we avoid thousands of per-batch Lucene flushes.
        send_final_commit(self.get_service_name())

        # All batches succeeded and the final commit is in — the checkpoint has
        # served its purpose. Remove it so the next run for this (phase, indexer)
        # pair starts clean (otherwise `partition_ids()` would skip everything).
        _clear_checkpoint(checkpoint_path)

        end_time = datetime.datetime.now()
        diff = end_time - start_time
        log.info(f"All data crawled and indexed in {diff.total_seconds() / 60.0} minutes")

        return all_data  # Return the combined dictionary

    def _process_chunk(
        self,
        chunk: List[str],
        label: str,
        vfb_json_query: str,
        neo4j_import_dir: str,
        jenkins_import_dir: str,
        checkpoint_path: str,
        all_data: Dict[str, Dict],
    ) -> None:
        """Run one chunk of ids through Neo4j → export file → Solr, updating
        the checkpoint on success.

        If ``execute_query`` exhausts its own retry budget (``Neo4jQueryException``)
        — typically because a single id in the chunk produces a result so
        large that apoc.export OOMs or the gateway times out — split the
        chunk in half and recurse. This isolates the heavy id(s) to a
        progressively smaller sub-chunk while the rest of the batch still
        gets indexed. Down to a single id we give up and re-raise, since
        there's nothing left to split.

        The per-sub-chunk label (``"7"`` → ``"7a"``, ``"7b"``, ``"7aa"`` …)
        keeps the export filenames unique so two recursive branches can't
        clobber each other's output.
        """
        output_filename = f"output_{label}.json"
        output_file_path = f"file://{neo4j_import_dir}/{output_filename}"
        exported_file_path = os.path.join(jenkins_import_dir, output_filename)

        try:
            self.execute_query(
                vfb_json_query, params={'ids': chunk}, output_file=output_file_path
            )
        except Neo4jQueryException as e:
            if len(chunk) <= 1:
                log.error(
                    f"{self.get_service_name()}: chunk '{label}' with single id "
                    f"{chunk!r} failed even after retries — giving up on it."
                )
                raise
            mid = len(chunk) // 2
            log.warning(
                f"{self.get_service_name()}: chunk '{label}' of {len(chunk)} ids "
                f"failed ({e}) — splitting in half to isolate heavy id(s) and retrying."
            )
            self._process_chunk(
                chunk[:mid], f"{label}a", vfb_json_query,
                neo4j_import_dir, jenkins_import_dir, checkpoint_path, all_data,
            )
            self._process_chunk(
                chunk[mid:], f"{label}b", vfb_json_query,
                neo4j_import_dir, jenkins_import_dir, checkpoint_path, all_data,
            )
            return

        # Wait for the file to be written
        while not os.path.exists(exported_file_path):
            time.sleep(1)

        # Process the file and collect the data
        batch_data = self.process_exported_file(exported_file_path)

        # Cache empty results: if Neo4j returned nothing for some ids in
        # this chunk, write a schema-compliant empty value to Solr so the
        # id shows as "existing" on the next run rather than being
        # re-queried forever. The empty format is defined per-indexer via
        # get_empty_result_json() — indexers that return None here signal
        # "don't cache empties" (e.g. term_info, where every id must have
        # a real result).
        service_name = self.get_service_name()
        empty_json = self.get_empty_result_json()
        if empty_json is not None:
            for doc_id in chunk:
                if doc_id and doc_id not in batch_data:
                    batch_data[doc_id] = {
                        "id": doc_id,
                        service_name: {"set": empty_json}
                    }

        # Write batch_data to Solr
        self.write_to_solr(batch_data)

        # Record the chunk as complete so a crash-and-restart skips it.
        # We record the *requested* chunk ids (not just batch_data keys) so
        # that ids with empty Neo4j results — or cached-empty ids written
        # above — are also treated as done and never re-requested.
        _append_checkpoint(checkpoint_path, chunk)

        # Update the all_data dictionary with the batch data
        all_data.update(batch_data)

        # Remove the file after processing
        os.remove(exported_file_path)

    def process_exported_file(self, file_path: str) -> Dict[str, Dict]:
        """
        Processes the exported JSON file and generates Solr documents.
        :param file_path: Path to the exported JSON file
        :return: Dictionary of Solr documents
        """
        batch_data = {}
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if line:
                    try:
                        result = json.loads(line)
                        solr_data = self.generate_solr_doc(result, request=None)
                        batch_data[solr_data["id"]] = solr_data
                    except json.JSONDecodeError as e:
                        log.error(f"JSON decoding error in file {file_path} at line {line_number}: {e}")
        return batch_data

    def generate_solr_doc(self, result: Dict, request: List[str]) -> Dict[str, Dict]:
        """
        Parses results and generates a Solr document to index using atomic updates.
        :param result: service response
        :param request: requests to get requested entity id if provided
        :return: Solr document in atomic update format
        """
        if self.REQUEST_BATCH_SIZE == 1 and request and request[0]:
            doc_id = request[0]
        elif "term" in result:
            doc_id = result["term"]["core"]["short_form"]
        elif "dataset" in result:
            doc_id = result["dataset"]["short_form"]
        elif "anatomy" in result:
            doc_id = result["anatomy"]["short_form"]
        else:
            raise ValueError("Unrecognised response data: " + json.dumps(result)[:50] + " ...")

        solr_doc = {
            "id": doc_id,
            self.get_service_name(): {"set": json.dumps(result)}
        }
        return solr_doc

    def get_query_parameters(self) -> List[str]:
        parameters = []
        query = self.get_parameters_query()
        if query:
            response = self.run_query(query)
            if response and "ids" in response[0]:
                parameters = response[0]["ids"]
        else:
            parameters.append("")
        return parameters

    def _wait_for_neo4j(self, deadline: float) -> bool:
        """Lightweight ping — polls Neo4j until it responds or deadline passes."""
        url = f"{self.nc.base_uri}{self.nc.commit}"
        # Send a trivial read-only query as a health check.
        ping_payload = json.dumps({'statements': [{'statement': 'RETURN 1'}]})
        headers = {'Content-Type': 'application/json'}

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            try:
                r = self._session.post(
                    url=url,
                    auth=(self.nc.usr, self.nc.pwd),
                    data=ping_payload,
                    headers=headers,
                    timeout=10,
                )
                if r.status_code == 200:
                    return True
                log.info(f"Neo4j ping returned {r.status_code}, waiting...")
                # Pinned backend responded unhealthy — drop affinity so the
                # next ping is routed to a different pod.
                self._drop_backend_affinity()
            except requests.exceptions.RequestException:
                self._drop_backend_affinity()

            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            sleep_time = min(NEO4J_HEALTH_CHECK_INTERVAL, remaining)
            log.info(
                f"Waiting {sleep_time:.0f}s for Neo4j to become available... "
                f"({remaining:.0f}s remaining before giving up)"
            )
            time.sleep(sleep_time)

    def execute_query(self, query: str, params: Dict = None, output_file: str = None) -> None:
        """
        Executes given Cypher query in Neo4j and exports the result to a file on the server.
        :param query: Cypher query to execute
        :param params: Query parameters
        :param output_file: Path to the output file on the Neo4j server (e.g., "file:///import/output.json")
        """
        # Escape double quotes in the query
        escaped_query = query.replace('"', '\\"')

        # Construct the export query
        export_query = f"""
        CALL apoc.export.json.query(
            "{escaped_query}",
            "{output_file}",
            {{batchSize: 20, params: $params}}
        )
        """

        # Prepare the statement with parameters
        cstatements = [{
            'statement': export_query,
            'parameters': {'params': params} if params else {}
        }]

        payload = json.dumps({'statements': cstatements})
        headers = {'Content-Type': 'application/json'}

        # Start the retry window only after the first failure so slow but
        # otherwise valid queries still get a full recovery budget.
        retry_deadline = None
        attempt = 0

        while True:
            try:
                response = self._session.post(
                    url=f"{self.nc.base_uri}{self.nc.commit}",
                    auth=(self.nc.usr, self.nc.pwd),
                    data=payload,
                    headers=headers
                )
                response.raise_for_status()
                response_json = response.json()
                if 'errors' in response_json and response_json['errors']:
                    log.error(f"Neo4j returned errors: {response_json['errors']}")
                    raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")
                log.info(f"Exported data For {params['ids'][0]} to {params['ids'][-1]}")
                return
            except requests.exceptions.RequestException as e:
                attempt += 1
                log.warning(str(e))
                self._drop_backend_affinity()

                if retry_deadline is None:
                    retry_deadline = time.time() + NEO4J_MAX_RETRY_WAIT_SECONDS

                remaining = retry_deadline - time.time()
                if remaining <= 0:
                    raise Neo4jQueryException(
                        f"{self.get_service_name()} query failed after {attempt} attempts "
                        f"({NEO4J_MAX_RETRY_WAIT_SECONDS}s deadline exceeded): {e}"
                    )

                log.info(
                    f"Waiting for Neo4j to become available before retrying "
                    f"({remaining:.0f}s remaining)..."
                )
                if not self._wait_for_neo4j(retry_deadline):
                    raise Neo4jQueryException(
                        f"Neo4j did not become available within "
                        f"{NEO4J_MAX_RETRY_WAIT_SECONDS}s for {self.get_service_name()}: {e}"
                    )

                # Brief back-off after ping succeeds.
                backoff = min(
                    NEO4J_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                    NEO4J_RETRY_MAX_DELAY_SECONDS,
                )
                backoff = min(backoff, retry_deadline - time.time())
                if backoff > 0:
                    log.info(
                        f"Neo4j is back. Retrying query for {self.get_service_name()} "
                        f"in {backoff:.0f}s (attempt {attempt + 1})."
                    )
                    time.sleep(backoff)

    def run_query(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Executes given Cypher query in Neo4j and returns the results.
        """
        cstatements = [{
            'statement': query,
            'parameters': params or {}
        }]

        payload = json.dumps({'statements': cstatements})
        headers = {'Content-Type': 'application/json'}

        # Start the retry window only after the first failure so slow but
        # otherwise valid queries still get a full recovery budget.
        retry_deadline = None
        attempt = 0

        while True:
            try:
                response = self._session.post(
                    url=f"{self.nc.base_uri}{self.nc.commit}",
                    auth=(self.nc.usr, self.nc.pwd),
                    data=payload,
                    headers=headers
                )
                response.raise_for_status()
                response_json = response.json()
                if 'errors' in response_json and response_json['errors']:
                    log.error(f"Neo4j returned errors: {response_json['errors']}")
                    raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")

                results = []
                for result in response_json['results']:
                    columns = result['columns']
                    for data_row in result['data']:
                        row = data_row['row']
                        result_dict = dict(zip(columns, row))
                        results.append(result_dict)
                return results
            except requests.exceptions.RequestException as e:
                attempt += 1
                log.warning(str(e))
                self._drop_backend_affinity()

                if retry_deadline is None:
                    retry_deadline = time.time() + NEO4J_MAX_RETRY_WAIT_SECONDS

                remaining = retry_deadline - time.time()
                if remaining <= 0:
                    raise Neo4jQueryException(
                        f"{self.get_service_name()} query failed after {attempt} attempts "
                        f"({NEO4J_MAX_RETRY_WAIT_SECONDS}s deadline exceeded): {e}"
                    )

                log.info(
                    f"Waiting for Neo4j to become available before retrying "
                    f"({remaining:.0f}s remaining)..."
                )
                if not self._wait_for_neo4j(retry_deadline):
                    raise Neo4jQueryException(
                        f"Neo4j did not become available within "
                        f"{NEO4J_MAX_RETRY_WAIT_SECONDS}s for {self.get_service_name()}: {e}"
                    )

                backoff = min(
                    NEO4J_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                    NEO4J_RETRY_MAX_DELAY_SECONDS,
                )
                backoff = min(backoff, retry_deadline - time.time())
                if backoff > 0:
                    log.info(
                        f"Neo4j is back. Retrying query for {self.get_service_name()} "
                        f"in {backoff:.0f}s (attempt {attempt + 1})."
                    )
                    time.sleep(backoff)

    def write_to_solr(self, solr_docs: Dict[str, Dict]) -> None:
        """
        Writes a batch of Solr documents to the Solr server using atomic updates.
        :param solr_docs: Dictionary of Solr documents to index.
        """
        send_solr_docs(solr_docs.values(), self.get_service_name())

    @abstractmethod
    def get_parameters_query(self) -> Optional[str]:
        """
        Cypher query to list short forms of all nodes that can be passed as parameters to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        pass

    @abstractmethod
    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        pass

    @abstractmethod
    def get_service_name(self) -> str:
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        pass

    def get_empty_result_json(self) -> Optional[str]:
        """Return the JSON string to cache in Solr for ids where the Neo4j
        query returned no results. This prevents those ids from appearing as
        "missing" on every future run.

        The value MUST conform to the schema that consumers of this service
        field expect. Override in each concrete indexer.

        Return ``None`` to skip caching empty results for this service —
        useful when every id is expected to have a real result (e.g.
        term_info) and an empty one would indicate a bug, not normal
        sparsity.

        =====================================================================
        Service                          | Empty format
        =====================================================================
        term_info                        | None  (always has results)
        anat_query                       | None  (always has results)
        anat_image_query                 | None  (always has results)
        anat_2_ep_query                  | '{}'  (vfb_query.json, no required fields)
        ep_2_anat_query                  | '{}'  (vfb_query.json, no required fields)
        template_2_datasets_query        | None  (always has results)
        all_datasets_query               | None  (always has results)
        anat_scRNAseq                    | '{}'  (vfb_query.json, no required fields)
        cluster_expression               | '{}'  (vfb_query.json, no required fields)
        downstream_connectivity_query    | '[]'  (list of connection dicts)
        upstream_connectivity_query      | '[]'  (list of connection dicts)
        =====================================================================
        """
        return None


class Neo4jQueryException(Exception):
    """
    Custom exception class.
    """
    pass


def get_chunks(lst: List, n: int) -> Generator:
    """
    Yield successive n-sized chunks from lst.
    :param lst: list to read by chunks
    :param n: max chunk size
    :return: chunk generator
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

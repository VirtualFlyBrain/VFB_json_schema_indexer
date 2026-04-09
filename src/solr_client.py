import json
import logging
import os
import time
from typing import Dict, Optional, Sequence, Set

import requests
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

# Retry / timeout configuration.
# Defaults retuned for the upgraded (high-RAM) Solr server: commits are no
# longer forced on every batch (see SOLR_COMMIT_WITHIN_MS), so individual
# writes are cheap and retries can start fast and back off exponentially.
SOLR_WRITE_MAX_RETRIES = int(os.getenv("SOLR_WRITE_MAX_RETRIES", 5))
SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS", 5))
SOLR_WRITE_RETRY_MAX_DELAY_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_MAX_DELAY_SECONDS", 120))
SOLR_WRITE_TIMEOUT_SECONDS = int(os.getenv("SOLR_WRITE_TIMEOUT_SECONDS", 180))

# Let Solr batch commits server-side rather than issuing a hard commit on every
# single POST. A final explicit commit is issued via send_final_commit() at the
# end of each service run.
SOLR_COMMIT_WITHIN_MS = int(os.getenv("SOLR_COMMIT_WITHIN_MS", 60000))

# Module-level session so TCP/TLS connections are reused across every POST.
# Without this the loader paid a full handshake on every ~150 batches.
_SESSION = requests.Session()
_ADAPTER = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
_SESSION.mount("http://", _ADAPTER)
_SESSION.mount("https://", _ADAPTER)


def get_solr_update_url() -> Optional[str]:
    solr_server = os.getenv("SOLRserver")
    solr_collection = os.getenv("SOLRcollection")

    if not solr_server or not solr_collection:
        log.error("SOLRserver or SOLRcollection environment variable is not set.")
        return None

    return f"{solr_server.rstrip('/')}/{solr_collection}/update"


def get_solr_select_url() -> Optional[str]:
    solr_server = os.getenv("SOLRserver")
    solr_collection = os.getenv("SOLRcollection")

    if not solr_server or not solr_collection:
        log.error("SOLRserver or SOLRcollection environment variable is not set.")
        return None

    return f"{solr_server.rstrip('/')}/{solr_collection}/select"


def fetch_ids_with_field(service_name: str) -> Set[str]:
    """Return the set of Solr doc ids that already have the given service field
    populated. Used to prioritise docs that are missing from Solr on the first
    pass of an indexer run.

    On any failure returns an empty set — callers should treat that as "nothing
    is known to exist", which preserves the original indexing order (all docs
    treated as missing) and never blocks the run.
    """
    solr_select_url = get_solr_select_url()
    if not solr_select_url:
        return set()

    params = {
        "q": f"{service_name}:[* TO *]",
        "fl": "id",
        "rows": "10000000",
        "wt": "json",
    }

    try:
        response = _SESSION.get(
            solr_select_url,
            params=params,
            timeout=SOLR_WRITE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        docs = data.get("response", {}).get("docs", [])
        ids = {doc["id"] for doc in docs if "id" in doc}
        log.info(
            f"Solr reports {len(ids)} existing documents for service '{service_name}'."
        )
        return ids
    except (requests.exceptions.RequestException, ValueError, KeyError) as exc:
        log.warning(
            f"Could not fetch existing ids for service '{service_name}': {exc}. "
            f"Proceeding without missing-first prioritisation."
        )
        return set()


def send_solr_docs(solr_docs: Sequence[Dict], service_name: str) -> bool:
    docs = list(solr_docs)
    if not docs:
        log.info(f"No documents to index to Solr for service '{service_name}'.")
        return True

    payload = json.dumps(docs)
    return send_solr_payload(payload, service_name=service_name, document_count=len(docs))


def send_solr_payload(
    payload: str,
    service_name: str,
    document_count: Optional[int] = None,
) -> bool:
    """POST a JSON payload to the Solr update endpoint with capped exponential
    retry. Commits are deferred via commitWithin; call send_final_commit() at
    the end of a service run to make changes searchable immediately.
    """
    solr_update_url = get_solr_update_url()
    if not solr_update_url:
        return False

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    params = {
        "commitWithin": str(SOLR_COMMIT_WITHIN_MS),
        "wt": "json",
    }

    log.debug(f"Solr update URL: {solr_update_url}")
    log.debug(f"Data being sent to Solr: {payload}")

    failure_scope = (
        f"{document_count} documents" if document_count is not None else "payload"
    )

    for attempt in range(SOLR_WRITE_MAX_RETRIES + 1):
        try:
            response = _SESSION.post(
                solr_update_url,
                params=params,
                data=payload,
                headers=headers,
                timeout=SOLR_WRITE_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            if document_count is None:
                log.info(f"Indexed Solr payload for service '{service_name}'.")
            else:
                log.info(
                    f"Indexed {document_count} documents to Solr for service '{service_name}'."
                )
            log.debug(f"Solr response: {response.text}")
            return True
        except requests.exceptions.RequestException as exc:
            response = getattr(exc, "response", None)
            log.warning(
                f"Failed to index {failure_scope} to Solr for service '{service_name}' "
                f"on attempt {attempt + 1}: {exc}"
            )
            if response is not None and response.text:
                log.error(f"Solr response: {response.text}")

            if attempt >= SOLR_WRITE_MAX_RETRIES:
                log.error(
                    f"Giving up indexing {failure_scope} to Solr for service "
                    f"'{service_name}' after {attempt + 1} attempts."
                )
                return False

            sleep_seconds = min(
                SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS * (2 ** attempt),
                SOLR_WRITE_RETRY_MAX_DELAY_SECONDS,
            )
            log.info(
                f"Retrying Solr write for service '{service_name}' in {sleep_seconds} seconds "
                f"(retry {attempt + 1} of {SOLR_WRITE_MAX_RETRIES})."
            )
            time.sleep(sleep_seconds)

    return False


def send_final_commit(service_name: str) -> bool:
    """Issue a single hard commit at the end of a service run so any docs
    still buffered by commitWithin become searchable immediately.
    """
    solr_update_url = get_solr_update_url()
    if not solr_update_url:
        return False

    params = {"commit": "true", "waitSearcher": "true", "wt": "json"}

    for attempt in range(SOLR_WRITE_MAX_RETRIES + 1):
        try:
            response = _SESSION.get(
                solr_update_url,
                params=params,
                timeout=SOLR_WRITE_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            log.info(f"Final commit issued for service '{service_name}'.")
            return True
        except requests.exceptions.RequestException as exc:
            log.warning(
                f"Final commit failed for service '{service_name}' on attempt "
                f"{attempt + 1}: {exc}"
            )
            if attempt >= SOLR_WRITE_MAX_RETRIES:
                log.error(
                    f"Giving up on final commit for service '{service_name}' "
                    f"after {attempt + 1} attempts."
                )
                return False
            sleep_seconds = min(
                SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS * (2 ** attempt),
                SOLR_WRITE_RETRY_MAX_DELAY_SECONDS,
            )
            time.sleep(sleep_seconds)

    return False

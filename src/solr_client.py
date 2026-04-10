import json
import logging
import os
import time
from typing import Dict, Iterable, Optional, Sequence, Set

import requests
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

# ---- Retry / timeout configuration ----
# Instead of a fixed retry count we use a wall-clock deadline so the loader
# can survive a full server restart (~20 min) without giving up.
SOLR_WRITE_TIMEOUT_SECONDS = int(os.getenv("SOLR_WRITE_TIMEOUT_SECONDS", 180))
SOLR_RETRY_INITIAL_DELAY_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS", 5))
SOLR_RETRY_MAX_DELAY_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_MAX_DELAY_SECONDS", 120))
SOLR_MAX_RETRY_WAIT_SECONDS = int(os.getenv("SOLR_MAX_RETRY_WAIT_SECONDS", 1200))  # 20 min
SOLR_HEALTH_CHECK_INTERVAL = 30  # seconds between pings while waiting for Solr

# Let Solr batch commits server-side rather than issuing a hard commit on every
# single POST. A final explicit commit is issued via send_final_commit() at the
# end of each service run.
SOLR_COMMIT_WITHIN_MS = int(os.getenv("SOLR_COMMIT_WITHIN_MS", 60000))

# Module-level session so TCP/TLS connections are reused across every POST.
_SESSION = requests.Session()
_ADAPTER = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
_SESSION.mount("http://", _ADAPTER)
_SESSION.mount("https://", _ADAPTER)


# ---- URL helpers ----

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


def _get_solr_ping_url() -> Optional[str]:
    solr_server = os.getenv("SOLRserver")
    solr_collection = os.getenv("SOLRcollection")
    if not solr_server or not solr_collection:
        return None
    return f"{solr_server.rstrip('/')}/{solr_collection}/admin/ping"


# ---- Health check ----

def _wait_for_solr(deadline: float, label: str = "") -> bool:
    """Lightweight ping loop — waits for Solr to respond 200 before we waste a
    full SOLR_WRITE_TIMEOUT_SECONDS on a payload POST to a dead server.

    Returns True once Solr is reachable, False if ``deadline`` is exceeded.
    """
    ping_url = _get_solr_ping_url()
    if not ping_url:
        return False

    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return False
        try:
            r = _SESSION.get(ping_url, params={"wt": "json"}, timeout=10)
            if r.status_code == 200:
                return True
            log.info(f"Solr ping returned {r.status_code}{f' ({label})' if label else ''}, waiting...")
        except requests.exceptions.RequestException:
            pass

        remaining = deadline - time.time()
        if remaining <= 0:
            return False
        sleep_time = min(SOLR_HEALTH_CHECK_INTERVAL, remaining)
        log.info(
            f"Waiting {sleep_time:.0f}s for Solr to become available"
            f"{f' ({label})' if label else ''}... "
            f"({remaining:.0f}s remaining before giving up)"
        )
        time.sleep(sleep_time)


# ---- Existence probe ----

SOLR_EXISTENCE_PROBE_CHUNK = int(os.getenv("SOLR_EXISTENCE_PROBE_CHUNK", 1000))


def fetch_ids_with_field(ids: Iterable[str], service_name: str) -> Set[str]:
    """Given a list of candidate ids, return the subset that already has the
    given service field populated in Solr.

    Probes by id because the per-service JSON payload fields are typically
    stored but not indexed. On any failure returns an empty set (preserves
    original ordering; never blocks the run).
    """
    solr_select_url = get_solr_select_url()
    if not solr_select_url:
        return set()

    id_list = [i for i in ids if i]
    if not id_list:
        return set()

    populated: Set[str] = set()

    try:
        for start in range(0, len(id_list), SOLR_EXISTENCE_PROBE_CHUNK):
            chunk = id_list[start:start + SOLR_EXISTENCE_PROBE_CHUNK]
            data = {
                "q": "{!terms f=id}" + ",".join(chunk),
                "fl": f"id,{service_name}",
                "rows": str(len(chunk)),
                "wt": "json",
            }
            response = _SESSION.post(
                solr_select_url,
                data=data,
                timeout=SOLR_WRITE_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            docs = response.json().get("response", {}).get("docs", [])
            for doc in docs:
                doc_id = doc.get("id")
                if not doc_id:
                    continue
                value = doc.get(service_name)
                if isinstance(value, list):
                    value = value[0] if value else None
                if value:
                    populated.add(doc_id)

        log.info(
            f"Solr reports {len(populated)}/{len(id_list)} existing documents "
            f"for service '{service_name}'."
        )
        return populated
    except (requests.exceptions.RequestException, ValueError, KeyError) as exc:
        log.warning(
            f"Could not probe existing ids for service '{service_name}': {exc}. "
            f"Proceeding without missing-first prioritisation."
        )
        return set()


# ---- Write helpers ----

def send_solr_docs(solr_docs: Sequence[Dict], service_name: str) -> bool:
    docs = list(solr_docs)
    if not docs:
        log.debug(f"No documents to index to Solr for service '{service_name}'.")
        return True

    payload = json.dumps(docs)
    payload_mb = len(payload) / (1024 * 1024)
    if payload_mb > 1:
        # Log individual doc sizes for large payloads to identify bloated documents.
        for doc in docs:
            doc_size = len(json.dumps(doc))
            doc_id = doc.get("id", "?")
            if doc_size > 1024 * 1024:  # > 1 MB
                log.warning(
                    f"Large document '{doc_id}' for service '{service_name}': "
                    f"{doc_size / (1024 * 1024):.2f} MB"
                )
        log.warning(
            f"Large payload for service '{service_name}': "
            f"{payload_mb:.2f} MB, {len(docs)} doc(s)"
        )
    else:
        log.debug(
            f"Payload for service '{service_name}': "
            f"{payload_mb:.2f} MB, {len(docs)} doc(s)"
        )
    return send_solr_payload(payload, service_name=service_name, document_count=len(docs))


def send_solr_payload(
    payload: str,
    service_name: str,
    document_count: Optional[int] = None,
) -> bool:
    """POST a JSON payload to the Solr update endpoint.

    On failure, waits for Solr to come back (via lightweight ping) then retries.
    Total wait time is bounded by SOLR_MAX_RETRY_WAIT_SECONDS (default 20 min)
    so the loader can survive a full server restart without giving up.
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

    failure_scope = (
        f"{document_count} documents" if document_count is not None else "payload"
    )

    deadline = time.time() + SOLR_MAX_RETRY_WAIT_SECONDS
    attempt = 0

    while True:
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
            return True
        except requests.exceptions.RequestException as exc:
            attempt += 1
            resp = getattr(exc, "response", None)
            log.warning(
                f"Failed to index {failure_scope} to Solr for service '{service_name}' "
                f"on attempt {attempt}: {exc}"
            )
            if resp is not None and resp.text:
                log.error(f"Solr response: {resp.text}")

            remaining = deadline - time.time()
            if remaining <= 0:
                log.error(
                    f"Giving up indexing {failure_scope} to Solr for service "
                    f"'{service_name}' after {attempt} attempts "
                    f"({SOLR_MAX_RETRY_WAIT_SECONDS}s deadline exceeded)."
                )
                return False

            # Wait for Solr to become reachable before retrying the real payload.
            log.info(
                f"Waiting for Solr to become available before retrying "
                f"({remaining:.0f}s remaining)..."
            )
            if not _wait_for_solr(deadline, label=service_name):
                log.error(
                    f"Solr did not become available within "
                    f"{SOLR_MAX_RETRY_WAIT_SECONDS}s for service '{service_name}'. "
                    f"Giving up after {attempt} attempts."
                )
                return False

            # Brief back-off after ping succeeds to let the core finish warming.
            backoff = min(
                SOLR_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                SOLR_RETRY_MAX_DELAY_SECONDS,
            )
            backoff = min(backoff, deadline - time.time())
            if backoff > 0:
                log.info(
                    f"Solr is back. Retrying write for service '{service_name}' "
                    f"in {backoff:.0f}s (attempt {attempt + 1})."
                )
                time.sleep(backoff)


def send_final_commit(service_name: str) -> bool:
    """Issue a single hard commit at the end of a service run."""
    solr_update_url = get_solr_update_url()
    if not solr_update_url:
        return False

    params = {"commit": "true", "waitSearcher": "true", "wt": "json"}
    deadline = time.time() + SOLR_MAX_RETRY_WAIT_SECONDS
    attempt = 0

    while True:
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
            attempt += 1
            log.warning(
                f"Final commit failed for service '{service_name}' on attempt "
                f"{attempt}: {exc}"
            )
            remaining = deadline - time.time()
            if remaining <= 0:
                log.error(
                    f"Giving up on final commit for service '{service_name}' "
                    f"after {attempt} attempts."
                )
                return False

            if not _wait_for_solr(deadline, label=f"{service_name} commit"):
                log.error(
                    f"Solr did not become available for final commit "
                    f"(service '{service_name}'). Giving up after {attempt} attempts."
                )
                return False

            backoff = min(
                SOLR_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                SOLR_RETRY_MAX_DELAY_SECONDS,
            )
            backoff = min(backoff, deadline - time.time())
            if backoff > 0:
                time.sleep(backoff)

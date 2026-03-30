import json
import logging
import os
import time
from typing import Dict, Optional, Sequence

import requests

log = logging.getLogger(__name__)

SOLR_WRITE_MAX_RETRIES = int(os.getenv("SOLR_WRITE_MAX_RETRIES", 10))
SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS", 30))
SOLR_WRITE_RETRY_DELAY_INCREMENT_SECONDS = int(os.getenv("SOLR_WRITE_RETRY_DELAY_INCREMENT_SECONDS", 15))
SOLR_WRITE_TIMEOUT_SECONDS = int(os.getenv("SOLR_WRITE_TIMEOUT_SECONDS", 60))


def get_solr_update_url() -> Optional[str]:
    solr_server = os.getenv("SOLRserver")
    solr_collection = os.getenv("SOLRcollection")

    if not solr_server or not solr_collection:
        log.error("SOLRserver or SOLRcollection environment variable is not set.")
        return None

    return f"{solr_server.rstrip('/')}/{solr_collection}/update"


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
    try_count: int = 0,
) -> bool:
    solr_update_url = get_solr_update_url()
    if not solr_update_url:
        return False

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    params = {
        "commit": "true",
        "wt": "json",
    }

    log.debug(f"Solr update URL: {solr_update_url}")
    log.debug(f"Data being sent to Solr: {payload}")

    try:
        response = requests.post(
            solr_update_url,
            params=params,
            data=payload,
            headers=headers,
            timeout=SOLR_WRITE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        if document_count is None:
            log.info(f"Indexed Solr payload for service '{service_name}' and committed changes.")
        else:
            log.info(
                f"Indexed {document_count} documents to Solr for service '{service_name}' "
                f"and committed changes."
            )
        log.debug(f"Solr response: {response.text}")
        return True
    except requests.exceptions.RequestException as exc:
        response = getattr(exc, "response", None)
        failure_scope = (
            f"{document_count} documents"
            if document_count is not None
            else "payload"
        )
        log.warning(
            f"Failed to index {failure_scope} to Solr for service '{service_name}' "
            f"on attempt {try_count + 1}: {exc}"
        )
        if response is not None and response.text:
            log.error(f"Solr response: {response.text}")
        if try_count < SOLR_WRITE_MAX_RETRIES:
            sleep_seconds = (
                SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS
                + try_count * SOLR_WRITE_RETRY_DELAY_INCREMENT_SECONDS
            )
            log.info(
                f"Retrying Solr write for service '{service_name}' in {sleep_seconds} seconds "
                f"(retry {try_count + 1} of {SOLR_WRITE_MAX_RETRIES})."
            )
            time.sleep(sleep_seconds)
            return send_solr_payload(
                payload,
                service_name=service_name,
                document_count=document_count,
                try_count=try_count + 1,
            )

        log.error(
            f"Giving up indexing {failure_scope} to Solr for service '{service_name}' "
            f"after {try_count + 1} attempts."
        )
        return False

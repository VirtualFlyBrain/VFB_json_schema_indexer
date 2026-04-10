import json
import logging
from typing import List, Dict, Optional

from src.indexers.base_query_indexer import BaseQueryIndexer

log = logging.getLogger(__name__)


class NeuronDownstreamConnectivityIndexer(BaseQueryIndexer):
    """
    Indexes downstream connectivity for each direct neuron class.

    For a given neuron class, finds all individual instances that are
    directly INSTANCEOF that class, follows their synapsed_to edges
    downstream, and groups the results by the downstream neuron's direct
    class.  One row per downstream partner class with summary statistics.

    No SUBCLASSOF expansion is done here — that is handled at query time
    by OWLERY.  The consumer fetches subclass short_forms from OWLERY,
    retrieves each from Solr, and merges the results client-side.
    """

    REQUEST_BATCH_SIZE = 1

    def get_service_name(self) -> str:
        return "downstream_connectivity_query"

    def get_empty_result_json(self) -> Optional[str]:
        return json.dumps([])

    def get_parameters_query(self) -> str:
        # Direct neuron classes whose instances participate in connectivity.
        return (
            "MATCH (c:Class:Neuron)"
            "<-[:INSTANCEOF]-(n:Individual:Neuron:has_neuron_connectivity)"
            "-[:database_cross_reference]->"
            "(s:Individual:Site {is_data_source:[True]}) "
            "WHERE NOT (s.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb']) "
            "AND NOT (s.symbol[0] IN ['hb', 'fafb']) "
            "RETURN collect(DISTINCT c.short_form) AS ids"
        )

    def get_vfb_json_query(self, ids: List[str]) -> str:
        ids_str = "['" + "', '".join(ids) + "']"
        return (
            f"MATCH (c1:Class:Neuron) WHERE c1.short_form IN {ids_str}\n"
            "WITH c1\n"
            # Direct instances of c1 that synapse onto instances of c2.
            "MATCH (c1)<-[:INSTANCEOF]-(n1:Individual:Neuron:has_neuron_connectivity)"
            "-[r:synapsed_to]->"
            "(n2:Individual:Neuron:has_neuron_connectivity)"
            "-[:INSTANCEOF]->(c2:Class:Neuron)\n"
            "WHERE r.weight[0] >= 0\n"
            "MATCH (n1)-[:database_cross_reference]->(s:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH c1, c2,\n"
            "     count(*) AS pairwise_connections,\n"
            "     sum(r.weight[0]) AS total_weight,\n"
            "     count(DISTINCT n1) AS connected_count\n"
            # Total instances of c1 in non-excluded connectomes (for percent_connected).
            "MATCH (c1)<-[:INSTANCEOF]-(all_n1:Individual:has_neuron_connectivity)"
            "-[:database_cross_reference]->"
            "(s2:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s2.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s2.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH c1, c2, pairwise_connections, total_weight,\n"
            "     connected_count, count(DISTINCT all_n1) AS total_c1_instances\n"
            "WITH c1, COLLECT({\n"
            "  downstream_class: c2.label,\n"
            "  downstream_class_id: c2.short_form,\n"
            "  total_upstream_count: total_c1_instances,\n"
            "  connected_upstream_count: connected_count,\n"
            "  percent_connected: round((toFloat(connected_count)/toFloat(total_c1_instances))*100),\n"
            "  pairwise_connections: pairwise_connections,\n"
            "  total_weight: total_weight,\n"
            "  average_weight: toFloat(total_weight)/pairwise_connections\n"
            "}) AS connections\n"
            "RETURN {class_id: c1.short_form, class_label: c1.label,\n"
            "        downstream_connections: connections} AS downstream"
        )

    def generate_solr_doc(self, result: Dict, request) -> Dict:
        data = result["downstream"]
        primary_id = data["class_id"]
        primary_label = data["class_label"]
        rows = []
        for conn in data.get("downstream_connections", []):
            rows.append({
                "query": "downstream_class_connectivity_query",
                "term": {
                    "core": {
                        "short_form": primary_id,
                        "label": primary_label,
                        "iri": "",
                        "types": ["Class", "Neuron"],
                        "unique_facets": ["Class", "Neuron"]
                    },
                    "description": [],
                    "comment": []
                },
                "object": {
                    "short_form": conn["downstream_class_id"],
                    "label": conn["downstream_class"],
                    "iri": "",
                    "types": ["Class", "Neuron"],
                    "unique_facets": ["Class", "Neuron"]
                },
                "class_connectivity": {
                    "upstream_class": primary_label,
                    "upstream_class_id": primary_id,
                    "downstream_class": conn["downstream_class"],
                    "downstream_class_id": conn["downstream_class_id"],
                    "total_upstream_count": conn["total_upstream_count"],
                    "connected_upstream_count": conn["connected_upstream_count"],
                    "percent_connected": conn["percent_connected"],
                    "pairwise_connections": conn["pairwise_connections"],
                    "total_weight": conn["total_weight"],
                    "average_weight": conn["average_weight"]
                }
            })
        return {
            "id": primary_id,
            self.get_service_name(): {"set": json.dumps(rows)}
        }

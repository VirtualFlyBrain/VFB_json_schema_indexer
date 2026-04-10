import json
import logging
from typing import List, Dict, Optional

from src.indexers.base_query_indexer import BaseQueryIndexer

log = logging.getLogger(__name__)


class NeuronUpstreamConnectivityIndexer(BaseQueryIndexer):
    """
    Indexes upstream connectivity for each direct neuron class.

    For a given neuron class, finds all individual instances that are
    directly INSTANCEOF that class, follows incoming synapsed_to edges
    from upstream neurons, and groups the results by the upstream neuron's
    direct class.  One row per upstream partner class with summary
    statistics.

    No SUBCLASSOF expansion is done here — that is handled at query time
    by OWLERY.  The consumer fetches subclass short_forms from OWLERY,
    retrieves each from Solr, and merges the results client-side.
    """

    REQUEST_BATCH_SIZE = 1

    def get_service_name(self) -> str:
        return "upstream_connectivity_query"

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
            f"MATCH (c2:Class:Neuron) WHERE c2.short_form IN {ids_str}\n"
            "WITH c2\n"
            # Upstream instances (n1) that synapse onto direct instances of c2.
            "MATCH (c1:Class:Neuron)"
            "<-[:INSTANCEOF]-(n1:Individual:Neuron:has_neuron_connectivity)"
            "-[r:synapsed_to]->"
            "(n2:Individual:Neuron:has_neuron_connectivity)"
            "-[:INSTANCEOF]->(c2)\n"
            "WHERE r.weight[0] >= 0\n"
            "MATCH (n1)-[:database_cross_reference]->(s:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH c2, c1,\n"
            "     count(*) AS pairwise_connections,\n"
            "     sum(r.weight[0]) AS total_weight,\n"
            "     count(DISTINCT n1) AS connected_count\n"
            # Total instances of the upstream partner class c1 (for percent_connected).
            "MATCH (c1)<-[:INSTANCEOF]-(all_n1:Individual:has_neuron_connectivity)"
            "-[:database_cross_reference]->"
            "(s2:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s2.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s2.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH c2, c1, pairwise_connections, total_weight,\n"
            "     connected_count, count(DISTINCT all_n1) AS total_c1_instances\n"
            "WITH c2, COLLECT({\n"
            "  upstream_class: c1.label,\n"
            "  upstream_class_id: c1.short_form,\n"
            "  total_upstream_count: total_c1_instances,\n"
            "  connected_upstream_count: connected_count,\n"
            "  percent_connected: round((toFloat(connected_count)/toFloat(total_c1_instances))*100),\n"
            "  pairwise_connections: pairwise_connections,\n"
            "  total_weight: total_weight,\n"
            "  average_weight: toFloat(total_weight)/pairwise_connections\n"
            "}) AS connections\n"
            "RETURN {class_id: c2.short_form, class_label: c2.label,\n"
            "        upstream_connections: connections} AS upstream"
        )

    def generate_solr_doc(self, result: Dict, request) -> Dict:
        data = result["upstream"]
        primary_id = data["class_id"]
        primary_label = data["class_label"]
        rows = []
        for conn in data.get("upstream_connections", []):
            rows.append({
                "query": "upstream_class_connectivity_query",
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
                    "short_form": conn["upstream_class_id"],
                    "label": conn["upstream_class"],
                    "iri": "",
                    "types": ["Class", "Neuron"],
                    "unique_facets": ["Class", "Neuron"]
                },
                "class_connectivity": {
                    "upstream_class": conn["upstream_class"],
                    "upstream_class_id": conn["upstream_class_id"],
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

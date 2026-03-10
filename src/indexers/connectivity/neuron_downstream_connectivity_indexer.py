import json
import logging
from typing import List, Dict

from src.indexers.base_query_indexer import BaseQueryIndexer

log = logging.getLogger(__name__)


class NeuronDownstreamConnectivityIndexer(BaseQueryIndexer):
    """
    Indexes downstream connectivity for each neuron class.
    For a given primary neuron class, collects all downstream partner classes
    (what the primary class connects TO) along with connection statistics.
    Produces one Solr doc per primary class.
    """

    REQUEST_BATCH_SIZE = 1

    def get_service_name(self) -> str:
        return "downstream_connectivity_query"

    def get_parameters_query(self) -> str:
        return "MATCH (n:Class:Neuron) RETURN collect(distinct n.short_form) as ids"

    def get_vfb_json_query(self, ids: List[str]) -> str:
        ids_str = "['" + "', '".join(ids) + "']"
        return (
            f"MATCH (primary:Class:Neuron) WHERE primary.short_form IN {ids_str} WITH primary\n"
            "MATCH (primary)<-[:SUBCLASSOF*0..]-(c1:Class:Neuron)\n"
            "MATCH (c2:Class:Neuron)\n"
            "MATCH (c1)<-[:INSTANCEOF]-(n1:Individual:Neuron:has_neuron_connectivity)"
            "-[r:synapsed_to]->"
            "(n2:Individual:Neuron:has_neuron_connectivity)-[:INSTANCEOF]->(c2)\n"
            "WHERE r.weight[0] >= 0\n"
            "MATCH (n1)-[:database_cross_reference]->(s:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH primary, c1, c2, count(*) AS pairwise_connections, "
            "sum(r.weight[0]) AS total_weight, count(DISTINCT n1) AS connected_upstream_count\n"
            "MATCH (c1)<-[:INSTANCEOF]-(all_n1:Individual:has_neuron_connectivity)"
            "-[:database_cross_reference]->(s2:Individual:Site {is_data_source:[True]})\n"
            "WHERE NOT (s2.short_form IN ['neuprint_JRC_Hemibrain_1point2point1', 'catmaid_fafb'])\n"
            "AND NOT (s2.symbol[0] IN ['hb', 'fafb'])\n"
            "WITH primary, c1, c2, pairwise_connections, total_weight, "
            "connected_upstream_count, count(DISTINCT all_n1) AS total_upstream_count\n"
            "WITH primary, COLLECT({\n"
            "  upstream_class: c1.label,\n"
            "  upstream_class_id: c1.short_form,\n"
            "  downstream_class: c2.label,\n"
            "  downstream_class_id: c2.short_form,\n"
            "  total_upstream_count: total_upstream_count,\n"
            "  connected_upstream_count: connected_upstream_count,\n"
            "  percent_connected: round((toFloat(connected_upstream_count)/toFloat(total_upstream_count))*100),\n"
            "  pairwise_connections: pairwise_connections,\n"
            "  total_weight: total_weight,\n"
            "  average_weight: toFloat(total_weight)/pairwise_connections\n"
            "}) AS connections\n"
            "RETURN {class_id: primary.short_form, class_label: primary.label, "
            "downstream_connections: connections} AS downstream"
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
                    "upstream_class": conn["upstream_class"],
                    "upstream_class_id": conn["upstream_class_id"],
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

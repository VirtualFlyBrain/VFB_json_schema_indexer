from src.indexers.base_query_indexer import BaseQueryIndexer
from typing import List, Optional


class Ep2AnatQueryIndexer(BaseQueryIndexer):

    REQUEST_BATCH_SIZE = 1

    def get_service_name(self) -> str:
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "ep_2_anat_query"

    def get_empty_result_json(self) -> Optional[str]:
        # vfb_query.json has no required fields; empty dict = "no anatomy for this expression pattern".
        return "{}"

    def get_parameters_query(self) -> str:
        """
        Cypyher query to to list short forms of all nodes that can be passed as parameter to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        return "MATCH (ep:Class:Expression_pattern)<-[ar:overlaps|part_of]-(:Individual)-[:INSTANCEOF]->(anat:Class) RETURN collect(distinct ep.short_form) as ids"

    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        return self.ql.ep_2_anat_query(ids)

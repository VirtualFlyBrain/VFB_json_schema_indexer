from src.indexers.base_query_indexer import BaseQueryIndexer


class AnatQueryIndexer(BaseQueryIndexer):

    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "anat_query"

    def get_parameters_query(self):
        """
        Cypyher query to list short forms of all nodes that can be passed as parameter to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        return "MATCH (n:Class:Anatomy) WHERE n.short_form STARTS WITH 'FBbt' AND NOT n:Deprecated RETURN collect(distinct n.short_form) as ids"

    def get_vfb_json_query(self, ids):
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        return self.ql.anat_query(short_forms=ids)

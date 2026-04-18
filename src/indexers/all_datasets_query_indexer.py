from src.indexers.base_query_indexer import BaseQueryIndexer
from typing import List, Optional


class AllDatasetsQueryIndexer(BaseQueryIndexer):

    REQUEST_BATCH_SIZE = 50

    def get_service_name(self) -> str:
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "all_datasets_query"

    def get_parameters_query(self) -> Optional[str]:
        """
        Returns None because the all_datasets_query Cypher already matches all
        DataSet nodes internally (``MATCH (ds:DataSet)``).  It does not
        reference the ``$ids`` parameter, so chunking template short-forms and
        re-running the same expensive query per chunk is redundant and risks a
        gateway timeout.  Returning None causes the base class to run the query
        exactly once.
        """
        return None

    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        return self.ql.all_datasets_query()

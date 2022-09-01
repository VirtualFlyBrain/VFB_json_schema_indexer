from src.indexers.base_query_indexer import BaseQueryIndexer
from typing import List


class ImagesQueryIndexer(BaseQueryIndexer):

    def get_service_name(self) -> str:
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "images_query"

    def get_parameters_query(self) -> str:
        """
        Cypyher query to list short forms of all nodes that can be passed as parameter to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        return "MATCH (n:Individual:has_image) RETURN collect(distinct n.short_form) as ids"

    def get_version_tag():
        tag = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'])
        return tag.decode(encoding='ascii').rstrip()

		
		
    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        return "MATCH (n:Individual:has_image)<-[:depicts]-(:Individual)-[r:in_register_with]->(:Template)-[:depicts]->(t:Template) WHERE n.short_form IN [%s] RETURN distinct n.short_form as id, collect({template:t.short_form,thumbnail:r.thumbnail,swc:r.swc,obj:r.obj,wlz:r.wlz,nrrd:r.nrrd}) as images" (short_forms=ids)

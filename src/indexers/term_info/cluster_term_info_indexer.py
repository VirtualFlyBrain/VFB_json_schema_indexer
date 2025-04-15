import logging
from typing import List

from src.indexers.base_query_indexer import BaseQueryIndexer

log = logging.getLogger(__name__)


class ClusterTermInfoQueryIndexer(BaseQueryIndexer):
    """
    Indexer for Cluster term info data.
    Uses the "Get JSON for Cluster" query to fetch information about clusters.
    """

    REQUEST_BATCH_SIZE = 10

    def get_service_name(self) -> str:
        return "term_info"

    def get_parameters_query(self) -> str:
        # Fetch all Cluster individuals
        return "MATCH (i:Individual:Cluster) RETURN collect(distinct i.short_form) as ids"

    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query for Cluster term info.
        :param ids: ids to query
        :return: query string
        """
        # Format the list of ids as a string for Cypher query
        ids_str = "['" + "', '".join(ids) + "']"
        
        # Simply format the query with proper line breaks for better readability
        # This preserves the original query logic while fixing the formatting issues
        return f"MATCH (primary:Cluster) WHERE primary.short_form in {ids_str} WITH primary" + """
        OPTIONAL MATCH (primary)-[:has_source]-(ds:DataSet)-[:has_license|license]->(l:License) 
        WITH COLLECT ({ dataset: { link : coalesce(ds.dataset_link[0], ''), core : { short_form: ds.short_form, label: coalesce(ds.label,''), iri: ds.iri, types: labels(ds), unique_facets: apoc.coll.sort(coalesce(ds.uniqueFacets, [])), symbol: coalesce(ds.symbol[0], '')} }, license: { icon : coalesce(l.license_logo[0], ''), link : coalesce(l.license_url[0], ''), core : { short_form: l.short_form, label: coalesce(l.label,''), iri: l.iri, types: labels(l), unique_facets: apoc.coll.sort(coalesce(l.uniqueFacets, [])), symbol: coalesce(l.symbol[0], '')} }}) AS dataset_license,primary 
        OPTIONAL MATCH (o:Class)<-[r:SUBCLASSOF|INSTANCEOF]-(primary) 
        WITH CASE WHEN o IS NULL THEN [] ELSE COLLECT ({ short_form: o.short_form, label: coalesce(o.label,''), iri: o.iri, types: labels(o), unique_facets: apoc.coll.sort(coalesce(o.uniqueFacets, [])), symbol: coalesce(o.symbol[0], '')} ) END AS parents ,primary,dataset_license 
        OPTIONAL MATCH (o:Class)<-[r {type:'Related'}]-(primary) 
        WITH CASE WHEN o IS NULL THEN [] ELSE COLLECT ({ relation: { label: r.label, iri: r.iri, type: type(r) } , object: { short_form: o.short_form, label: coalesce(o.label,''), iri: o.iri, types: labels(o), unique_facets: apoc.coll.sort(coalesce(o.uniqueFacets, [])), symbol: coalesce(o.symbol[0], '')} }) END AS relationships ,primary,dataset_license,parents 
        OPTIONAL MATCH (s:Site { short_form: apoc.convert.toList(primary.self_xref)[0] }) 
        WITH CASE WHEN s IS NULL THEN [] ELSE COLLECT({ link_base: coalesce(s.link_base[0], ''), accession: coalesce(primary.short_form, ''), link_text: primary.label + ' on ' + s.label, homepage: coalesce(s.homepage[0], ''), site: { short_form: s.short_form, label: coalesce(s.label,''), iri: s.iri, types: labels(s), unique_facets: apoc.coll.sort(coalesce(s.uniqueFacets, [])), symbol: coalesce(s.symbol[0], '')} , icon: coalesce(s.link_icon_url[0], ''), link_postfix: coalesce(s.link_postfix[0], '')}) END AS self_xref, primary, dataset_license, parents, relationships 
        OPTIONAL MATCH (s:Site)<-[dbx:database_cross_reference]-(primary) 
        WITH CASE WHEN s IS NULL THEN self_xref ELSE COLLECT({ link_base: coalesce(s.link_base[0], ''), accession: coalesce(dbx.accession[0], ''), link_text: primary.label + ' on ' + s.label, homepage: coalesce(s.homepage[0], ''), site: { short_form: s.short_form, label: coalesce(s.label,''), iri: s.iri, types: labels(s), unique_facets: apoc.coll.sort(coalesce(s.uniqueFacets, [])), symbol: coalesce(s.symbol[0], '')} , icon: coalesce(s.link_icon_url[0], ''), link_postfix: coalesce(s.link_postfix[0], '')}) + self_xref END AS xrefs,primary,dataset_license,parents,relationships 
        OPTIONAL MATCH (primary)<-[:depicts]-(channel:Individual)-[irw:in_register_with]->(template:Individual)-[:depicts]->(template_anat:Individual) 
        WITH template, channel, template_anat, irw, primary, dataset_license, parents, relationships, xrefs 
        OPTIONAL MATCH (channel)-[:is_specified_output_of]->(technique:Class) 
        WITH CASE WHEN channel IS NULL THEN [] ELSE collect ({ channel: { short_form: channel.short_form, label: coalesce(channel.label,''), iri: channel.iri, types: labels(channel), unique_facets: apoc.coll.sort(coalesce(channel.uniqueFacets, [])), symbol: coalesce(channel.symbol[0], '')} , imaging_technique: { short_form: technique.short_form, label: coalesce(technique.label,''), iri: technique.iri, types: labels(technique), unique_facets: apoc.coll.sort(coalesce(technique.uniqueFacets, [])), symbol: coalesce(technique.symbol[0], '')} ,image: { template_channel : { short_form: template.short_form, label: coalesce(template.label,''), iri: template.iri, types: labels(template), unique_facets: apoc.coll.sort(coalesce(template.uniqueFacets, [])), symbol: coalesce(template.symbol[0], '')} , template_anatomy: { short_form: template_anat.short_form, label: coalesce(template_anat.label,''), iri: template_anat.iri, types: labels(template_anat), unique_facets: apoc.coll.sort(coalesce(template_anat.uniqueFacets, [])), symbol: coalesce(template_anat.symbol[0], '')} ,image_folder: COALESCE(irw.folder[0], ''), index: coalesce(apoc.convert.toInteger(irw.index[0]), []) + [] }}) END AS channel_image,primary,dataset_license,parents,relationships,xrefs 
        OPTIONAL MATCH (o:Individual)<-[r {type:'Related'}]-(primary) 
        WITH CASE WHEN o IS NULL THEN [] ELSE COLLECT ({ relation: { label: r.label, iri: r.uri, type: type(r) } , object: { short_form: o.short_form, label: coalesce(o.label,''), iri: o.iri, unique_facets: apoc.coll.sort(coalesce(o.uniqueFacets, [])), symbol: coalesce(o.symbol[0], ''), types: labels(o) }  }) END AS related_individuals ,primary,dataset_license,parents,relationships,xrefs,channel_image 
        RETURN { core : { short_form: primary.short_form, label: coalesce(primary.label,''), iri: primary.iri, types: labels(primary) , unique_facets: apoc.coll.sort(coalesce(primary.uniqueFacets, [])), symbol: coalesce(primary.symbol[0], '')}, description: coalesce(primary.description, []), comment : coalesce(primary.comment, primary.`annotation-comment`, []) } AS term, 'Get JSON for Cluster using modified Individual:Anatomy' AS query, 'ca9ab19' AS version , dataset_license, parents, relationships, xrefs, channel_image, related_individuals
        """

    def generate_solr_doc(self, result: dict, request=None) -> dict:
        """
        Generates a solr document from a cluster term info result.
        
        :param result: VFB JSON API response
        :param request: Request object
        :return: Dictionary containing solr document
        """
        solr_doc = {}
        solr_doc["id"] = result["term"]["core"]["short_form"]
        solr_doc[self.get_service_name()] = {"set": result}
        return solr_doc
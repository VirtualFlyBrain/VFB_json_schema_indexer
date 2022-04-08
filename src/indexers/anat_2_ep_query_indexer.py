from src.indexers.base_query_indexer import BaseQueryIndexer


class Anat2EpQueryIndexer(BaseQueryIndexer):
    """
    Example query, we can delete this comment later

    MATCH (ep:Class:Expression_pattern)<-[ar:overlaps|part_of]-(:Individual)-[:INSTANCEOF]->(anat:Class) WHERE anat.short_form in ['FBbt_00050101', 'FBbt_00050253', 'FBbt_00050143', 'FBbt_00050167', 'FBbt_00110412', 'FBbt_00100218', 'FBbt_00003638', 'FBbt_00003662', 'FBbt_00003641', 'FBbt_00003639', 'FBbt_00110325', 'FBbt_00111506', 'FBbt_00111052', 'FBbt_00111507', 'FBbt_00111508', 'FBbt_00111053', 'FBbt_00111509', 'FBbt_00111510', 'FBbt_00111055', 'FBbt_00111054', 'FBbt_00040033', 'FBbt_00110151', 'FBbt_00003646', 'FBbt_00003643', 'FBbt_00110326', 'FBbt_00007566', 'FBbt_00007565', 'FBbt_00007564', 'FBbt_00007563', 'FBbt_00007562', 'FBbt_00007561', 'FBbt_00007560', 'FBbt_00007559', 'FBbt_00003634'] WITH DISTINCT collect(DISTINCT ar.pub) as pubs, anat, ep UNWIND pubs as p MATCH (pub:pub { short_form: p})
    WITH anat, ep, collect({ core: { short_form: pub.short_form, label: coalesce(pub.label,''), iri: pub.iri, types: labels(pub), unique_facets: apoc.coll.sort(coalesce(pub.uniqueFacets, [])), symbol: coalesce(pub.symbol[0], '')} , PubMed: coalesce(pub.PMID[0], ''), FlyBase: coalesce(pub.FlyBase[0], ''), DOI: coalesce(pub.DOI[0], '') }) as pubs
    CALL apoc.cypher.run('WITH ep OPTIONAL MATCH (ep)<- [:has_source|SUBCLASSOF|INSTANCEOF*]-(i:Individual)<-[:depicts]- (channel:Individual)-[irw:in_register_with] ->(template:Individual)-[:depicts]-> (template_anat:Individual) RETURN template, channel, template_anat, i, irw limit 10', {ep:ep}) yield value with value.template as template, value.channel as channel,value.template_anat as template_anat, value.i as i, value.irw as irw, anat, ep, pubs OPTIONAL MATCH (channel)-[:is_specified_output_of]->(technique:Class)
    WITH CASE WHEN channel IS NULL THEN [] ELSE COLLECT({ anatomy: { short_form: i.short_form, label: coalesce(i.label,''), iri: i.iri, types: labels(i), unique_facets: apoc.coll.sort(coalesce(i.uniqueFacets, [])), symbol: coalesce(i.symbol[0], '')} , channel_image: { channel: { short_form: channel.short_form, label: coalesce(channel.label,''), iri: channel.iri, types: labels(channel), unique_facets: apoc.coll.sort(coalesce(channel.uniqueFacets, [])), symbol: coalesce(channel.symbol[0], '')} , imaging_technique: { short_form: technique.short_form, label: coalesce(technique.label,''), iri: technique.iri, types: labels(technique), unique_facets: apoc.coll.sort(coalesce(technique.uniqueFacets, [])), symbol: coalesce(technique.symbol[0], '')} ,image: { template_channel : { short_form: template.short_form, label: coalesce(template.label,''), iri: template.iri, types: labels(template), unique_facets: apoc.coll.sort(coalesce(template.uniqueFacets, [])), symbol: coalesce(template.symbol[0], '')} , template_anatomy: { short_form: template_anat.short_form, label: coalesce(template_anat.label,''), iri: template_anat.iri, types: labels(template_anat), unique_facets: apoc.coll.sort(coalesce(template_anat.uniqueFacets, [])), symbol: coalesce(template_anat.symbol[0], '')} ,image_folder: COALESCE(irw.folder[0], ''), index: coalesce(apoc.convert.toInteger(irw.index[0]), []) + [] }} }) END AS anatomy_channel_image ,anat,ep,pubs
    RETURN { short_form: anat.short_form, label: coalesce(anat.label,''), iri: anat.iri, types: labels(anat), unique_facets: apoc.coll.sort(coalesce(anat.uniqueFacets, [])), symbol: coalesce(anat.symbol[0], '')}  as anatomy, { short_form: ep.short_form, label: coalesce(ep.label,''), iri: ep.iri, types: labels(ep), unique_facets: apoc.coll.sort(coalesce(ep.uniqueFacets, [])), symbol: coalesce(ep.symbol[0], '')}  AS expression_pattern, 'Get JSON for anat_2_ep query' AS query, '076b8f3' AS version , pubs, anatomy_channel_image

    """

    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "anat_2_ep_query"

    def get_parameters_query(self):
        """
        Cypyher query to to list short forms of all nodes that can be passed as parameter to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        # return "MATCH (n:has_image:Individual) RETURN collect(distinct n.short_form) as ids"

    def get_vfb_json_query(self, ids):
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        return self.ql.anat_2_ep_query(short_forms=ids)

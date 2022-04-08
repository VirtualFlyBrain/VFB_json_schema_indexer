from src.indexers.base_query_indexer import BaseQueryIndexer


class Template2DatasetsQueryIndexer(BaseQueryIndexer):
    """
    Example query, we can delete this comment later

    MATCH (t:Template)<-[depicts]-(tc:Template)-[:in_register_with]-(c:Individual)-[:depicts]->(ai:Individual)-[:has_source]->(ds:DataSet) WHERE t.short_form in ['VFB_00050000']
    WITH distinct ds
    CALL apoc.cypher.run('WITH ds OPTIONAL MATCH (ds)<- [:has_source|SUBCLASSOF|INSTANCEOF*]-(i:Individual)<-[:depicts]- (channel:Individual)-[irw:in_register_with] ->(template:Individual)-[:depicts]-> (template_anat:Individual) RETURN template, channel, template_anat, i, irw limit 10', {ds:ds}) yield value with value.template as template, value.channel as channel,value.template_anat as template_anat, value.i as i, value.irw as irw, ds OPTIONAL MATCH (channel)-[:is_specified_output_of]->(technique:Class)
    WITH CASE WHEN channel IS NULL THEN [] ELSE COLLECT({ anatomy: { short_form: i.short_form, label: coalesce(i.label,''), iri: i.iri, types: labels(i), unique_facets: apoc.coll.sort(coalesce(i.uniqueFacets, [])), symbol: coalesce(i.symbol[0], '')} , channel_image: { channel: { short_form: channel.short_form, label: coalesce(channel.label,''), iri: channel.iri, types: labels(channel), unique_facets: apoc.coll.sort(coalesce(channel.uniqueFacets, [])), symbol: coalesce(channel.symbol[0], '')} , imaging_technique: { short_form: technique.short_form, label: coalesce(technique.label,''), iri: technique.iri, types: labels(technique), unique_facets: apoc.coll.sort(coalesce(technique.uniqueFacets, [])), symbol: coalesce(technique.symbol[0], '')} ,image: { template_channel : { short_form: template.short_form, label: coalesce(template.label,''), iri: template.iri, types: labels(template), unique_facets: apoc.coll.sort(coalesce(template.uniqueFacets, [])), symbol: coalesce(template.symbol[0], '')} , template_anatomy: { short_form: template_anat.short_form, label: coalesce(template_anat.label,''), iri: template_anat.iri, types: labels(template_anat), unique_facets: apoc.coll.sort(coalesce(template_anat.uniqueFacets, [])), symbol: coalesce(template_anat.symbol[0], '')} ,image_folder: COALESCE(irw.folder[0], ''), index: coalesce(apoc.convert.toInteger(irw.index[0]), []) + [] }} }) END AS anatomy_channel_image ,ds
    OPTIONAL MATCH (ds)-[rp:has_reference]->(p:pub)
    WITH CASE WHEN p is null THEN [] ELSE collect({ core: { short_form: p.short_form, label: coalesce(p.label,''), iri: p.iri, types: labels(p), unique_facets: apoc.coll.sort(coalesce(p.uniqueFacets, [])), symbol: coalesce(p.symbol[0], '')} , PubMed: coalesce(p.PMID[0], ''), FlyBase: coalesce(p.FlyBase[0], ''), DOI: coalesce(p.DOI[0], '') } ) END AS pubs,ds,anatomy_channel_image
    OPTIONAL MATCH (ds)-[:has_license|license]->(l:License)
    WITH collect ({ icon : coalesce(l.license_logo[0], ''), link : coalesce(l.license_url[0], ''), core : { short_form: l.short_form, label: coalesce(l.label,''), iri: l.iri, types: labels(l), unique_facets: apoc.coll.sort(coalesce(l.uniqueFacets, [])), symbol: coalesce(l.symbol[0], '')}  }) as license,ds,anatomy_channel_image,pubs
    OPTIONAL MATCH (ds)<-[:has_source]-(i:Individual) WITH  i, ds, anatomy_channel_image, pubs, license OPTIONAL MATCH (i)-[:INSTANCEOF]-(c:Class)
    WITH DISTINCT { images: count(distinct i),types: count(distinct c) } as dataset_counts,ds,anatomy_channel_image,pubs,license
    RETURN { short_form: ds.short_form, label: coalesce(ds.label,''), iri: ds.iri, types: labels(ds), unique_facets: apoc.coll.sort(coalesce(ds.uniqueFacets, [])), symbol: coalesce(ds.symbol[0], '')}  as dataset, '076b8f3' AS version , anatomy_channel_image, pubs, license, dataset_counts

    """

    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "template_2_datasets_query"

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
        return self.ql.template_2_datasets_query(short_forms=ids)

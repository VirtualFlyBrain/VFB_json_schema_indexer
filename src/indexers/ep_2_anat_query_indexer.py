from src.indexers.base_query_indexer import BaseQueryIndexer


class Ep2AnatQueryIndexer(BaseQueryIndexer):
    """
    Example query, we can delete this comment later

    MATCH (ep:Expression_pattern:Class)<-[ar:overlaps|part_of]-(anoni:Individual)-[:INSTANCEOF]->(anat:Class) WHERE ep.short_form in ['VFBexp_FBtp0106753'] WITH  anoni, anat, ar OPTIONAL MATCH (p:pub { short_form: ar.pub})
    WITH anat, anoni, { core: { short_form: p.short_form, label: coalesce(p.label,''), iri: p.iri, types: labels(p), unique_facets: apoc.coll.sort(coalesce(p.uniqueFacets, [])), symbol: coalesce(p.symbol[0], '')} , PubMed: coalesce(p.PMID[0], ''), FlyBase: coalesce(p.FlyBase[0], ''), DOI: coalesce(p.DOI[0], '') } AS pub
    OPTIONAL MATCH (anoni)-[r:Related]->(o:FBdv)
    WITH CASE WHEN o IS NULL THEN [] ELSE COLLECT ({ relation: { label: r.label, iri: r.iri, type: type(r) } , object: { short_form: o.short_form, label: coalesce(o.label,''), iri: o.iri, types: labels(o), unique_facets: apoc.coll.sort(coalesce(o.uniqueFacets, [])), symbol: coalesce(o.symbol[0], '')}  }) END AS stages ,anoni,anat,pub
    CALL apoc.cypher.run('WITH anat OPTIONAL MATCH (anat:Synaptic_neuropil)<- [:has_source|SUBCLASSOF|INSTANCEOF*]-(i:Individual)<-[:depicts]- (channel:Individual)-[irw:in_register_with] ->(template:Individual)-[:depicts]-> (template_anat:Individual) RETURN template, channel, template_anat, i, irw limit 10', {anat:anat}) yield value with value.template as template, value.channel as channel,value.template_anat as template_anat, value.i as i, value.irw as irw, anoni, anat, pub, stages OPTIONAL MATCH (channel)-[:is_specified_output_of]->(technique:Class)
    WITH CASE WHEN channel IS NULL THEN [] ELSE COLLECT({ anatomy: { short_form: i.short_form, label: coalesce(i.label,''), iri: i.iri, types: labels(i), unique_facets: apoc.coll.sort(coalesce(i.uniqueFacets, [])), symbol: coalesce(i.symbol[0], '')} , channel_image: { channel: { short_form: channel.short_form, label: coalesce(channel.label,''), iri: channel.iri, types: labels(channel), unique_facets: apoc.coll.sort(coalesce(channel.uniqueFacets, [])), symbol: coalesce(channel.symbol[0], '')} , imaging_technique: { short_form: technique.short_form, label: coalesce(technique.label,''), iri: technique.iri, types: labels(technique), unique_facets: apoc.coll.sort(coalesce(technique.uniqueFacets, [])), symbol: coalesce(technique.symbol[0], '')} ,image: { template_channel : { short_form: template.short_form, label: coalesce(template.label,''), iri: template.iri, types: labels(template), unique_facets: apoc.coll.sort(coalesce(template.uniqueFacets, [])), symbol: coalesce(template.symbol[0], '')} , template_anatomy: { short_form: template_anat.short_form, label: coalesce(template_anat.label,''), iri: template_anat.iri, types: labels(template_anat), unique_facets: apoc.coll.sort(coalesce(template_anat.uniqueFacets, [])), symbol: coalesce(template_anat.symbol[0], '')} ,image_folder: COALESCE(irw.folder[0], ''), index: coalesce(apoc.convert.toInteger(irw.index[0]), []) + [] }} }) END AS anatomy_channel_image ,anoni,anat,pub,stages
    RETURN { short_form: anat.short_form, label: coalesce(anat.label,''), iri: anat.iri, types: labels(anat), unique_facets: apoc.coll.sort(coalesce(anat.uniqueFacets, [])), symbol: coalesce(anat.symbol[0], '')}  AS anatomy, 'Get JSON for ep_2_anat query' AS query, '076b8f3' AS version , pub, stages, anatomy_channel_image

    """

    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "ep_2_anat_query"

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
        return self.ql.ep_2_anat_query(short_forms=ids)

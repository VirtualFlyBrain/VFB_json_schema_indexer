from src.indexers.base_query_indexer import BaseQueryIndexer


class AnatQueryIndexer(BaseQueryIndexer):
    """
    Example query, we can delete this comment later

    MATCH (primary:Class:Anatomy) WHERE primary.short_form in ['FBbt_00048350', 'FBbt_00047826', 'FBbt_00111338', 'FBbt_00048346', 'FBbt_00047227', 'FBbt_00007445', 'FBbt_00003864', 'FBbt_00007452', 'FBbt_00111354', 'FBbt_00007476', 'FBbt_00111661', 'FBbt_00049514', 'FBbt_00100470', 'FBbt_00100225', 'FBbt_00110570', 'FBbt_00111685', 'FBbt_00047723', 'FBbt_00110086', 'FBbt_00067349', 'FBbt_00007444', 'FBbt_00048354', 'FBbt_00003879', 'FBbt_00111481', 'FBbt_00067369', 'FBbt_00111359', 'FBbt_00067350', 'FBbt_00111749', 'FBbt_00100485', 'FBbt_00003994', 'FBbt_00007437', 'FBbt_00110427', 'FBbt_00110983', 'FBbt_00047681', 'FBbt_00067123', 'FBbt_00111715', 'FBbt_00047724', 'FBbt_00047825', 'FBbt_00047429', 'FBbt_00048353', 'FBbt_00111470', 'FBbt_00003875', 'FBbt_00067021', 'FBbt_00100381', 'FBbt_00111355', 'FBbt_00047720', 'FBbt_00067364', 'FBbt_00100388', 'FBbt_00048520', 'FBbt_00003880', 'FBbt_00100489']
    WITH primary
    CALL apoc.cypher.run('WITH primary OPTIONAL MATCH (primary)<- [:has_source|SUBCLASSOF|INSTANCEOF*]-(i:Individual)<-[:depicts]- (channel:Individual)-[irw:in_register_with] ->(template:Individual)-[:depicts]-> (template_anat:Individual) RETURN template, channel, template_anat, i, irw limit 10', {primary:primary}) yield value with value.template as template, value.channel as channel,value.template_anat as template_anat, value.i as i, value.irw as irw, primary OPTIONAL MATCH (channel)-[:is_specified_output_of]->(technique:Class)
    WITH CASE WHEN channel IS NULL THEN [] ELSE COLLECT({ anatomy: { short_form: i.short_form, label: coalesce(i.label,''), iri: i.iri, types: labels(i), unique_facets: apoc.coll.sort(coalesce(i.uniqueFacets, [])), symbol: coalesce(i.symbol[0], '')} , channel_image: { channel: { short_form: channel.short_form, label: coalesce(channel.label,''), iri: channel.iri, types: labels(channel), unique_facets: apoc.coll.sort(coalesce(channel.uniqueFacets, [])), symbol: coalesce(channel.symbol[0], '')} , imaging_technique: { short_form: technique.short_form, label: coalesce(technique.label,''), iri: technique.iri, types: labels(technique), unique_facets: apoc.coll.sort(coalesce(technique.uniqueFacets, [])), symbol: coalesce(technique.symbol[0], '')} ,image: { template_channel : { short_form: template.short_form, label: coalesce(template.label,''), iri: template.iri, types: labels(template), unique_facets: apoc.coll.sort(coalesce(template.uniqueFacets, [])), symbol: coalesce(template.symbol[0], '')} , template_anatomy: { short_form: template_anat.short_form, label: coalesce(template_anat.label,''), iri: template_anat.iri, types: labels(template_anat), unique_facets: apoc.coll.sort(coalesce(template_anat.uniqueFacets, [])), symbol: coalesce(template_anat.symbol[0], '')} ,image_folder: COALESCE(irw.folder[0], ''), index: coalesce(apoc.convert.toInteger(irw.index[0]), []) + [] }} }) END AS anatomy_channel_image ,primary
    RETURN { core : { short_form: primary.short_form, label: coalesce(primary.label,''), iri: primary.iri, types: labels(primary), unique_facets: apoc.coll.sort(coalesce(primary.uniqueFacets, [])), symbol: coalesce(primary.symbol[0], '')} , description : coalesce(primary.description, []), comment : coalesce(primary.comment, []) } AS term, '076b8f3' AS version , anatomy_channel_image

    """

    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        return "anat_query"

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
        return self.ql.anat_query(short_forms=ids)

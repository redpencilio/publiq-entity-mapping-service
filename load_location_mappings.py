from string import Template
import datetime

from helpers import query, update, generate_uuid
from escape_helpers import (sparql_escape_string,
    sparql_escape_uri,
    sparql_escape_datetime,
    sparql_escape_float)
from query_result_helpers import to_recs

MAPPING_GRAPH = "http://mu.semte.ch/graphs/entity-mappings"
MANUAL_MAPPING_GRAPH = "http://mu.semte.ch/graphs/entity-manual-mappings"
CLUSTER_GRAPH = "http://mu.semte.ch/graphs/entity-clusters"

def load_ungrouped_location_mapping():
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX locn: <http://www.w3.org/ns/locn#>
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?location ?mapping ?a ?b
WHERE {
    GRAPH $manual_mapping_graph {
        ?mapping a sssom:Mapping ;
            sssom:predicate_id ?predicate_id .
            # TODO: determine if both are ok
            VALUES ?predicate_id {
                <http://www.w3.org/2004/02/skos/core#exactMatch>
                <http://www.w3.org/2004/02/skos/core#relatedMatch>
                # 'http://mu.semte.ch/vocabularies/ext/noMatch'
            }
        {
            ?mapping sssom:subject_id ?location .
            ?mapping sssom:object_id ?b .
        }
        UNION
        {
            ?mapping sssom:subject_id ?a .
            ?mapping sssom:object_id ?location .
        }
    }
    {
        SELECT ?location
        WHERE {
            VALUES ?g {
                <http://locatieslinkeddata.ticketgang-locations.ticketing.acagroup.be>
                <http://locatiessparql.kunstenpunt-locaties.professionelekunsten.kunsten.be>
                <http://placessparql.publiq-uit-locaties.vrijetijdsparticipatie.publiq.be>
                <http://organisatorensparql.publiq-uit-organisatoren.vrijetijdsparticipatie.publiq.be>
            }
            GRAPH ?g {
                ?location a dct:Location .
            }
            GRAPH $manual_mapping_graph {
                # For now clusters of mappings. These may become clusters of locations?
                ?m a sssom:Mapping .
                ?m sssom:subject_id ?location .
            }
            GRAPH $cluster_graph {
                FILTER NOT EXISTS {
                    ?clocation a ext:Cluster .
                    ?clocation ext:member ?m .
                }
            }
        }
        LIMIT 1
    }
}
LIMIT 1
""")
    query_string = query_template.substitute(
        manual_mapping_graph=sparql_escape_uri(MANUAL_MAPPING_GRAPH),
        cluster_graph=sparql_escape_uri(CLUSTER_GRAPH)
    )
    query_result = query(query_string)
    if query_result["results"]["bindings"]:
        return to_recs(query_result)[0]
    else:
        return None


def query_related_mappings(mapping, related_mappings=[]):
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX locn: <http://www.w3.org/ns/locn#>
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?related_mapping
WHERE {
    {
        SELECT DISTINCT ?location
        WHERE {
            GRAPH $manual_mapping_graph {
                $mapping a sssom:Mapping ;
                    sssom:predicate_id ?predicate_id .
                VALUES ?predicate_id {
                    <http://www.w3.org/2004/02/skos/core#exactMatch>
                    <http://www.w3.org/2004/02/skos/core#relatedMatch>
                    # 'http://mu.semte.ch/vocabularies/ext/noMatch'
                }
                {
                    $mapping sssom:subject_id ?location .
                }
                UNION
                {
                    $mapping sssom:object_id ?location .
                }
            }
        }
    }
    GRAPH $manual_mapping_graph {
        ?related_mapping a sssom:Mapping .
        {
            ?related_mapping sssom:subject_id ?location .
        }
        UNION
        {
            ?related_mapping sssom:object_id ?location .
        }
        FILTER (?related_mapping NOT IN (
            $related_mappings
        ))
    }
}
""")
    query_string = query_template.substitute(
        mapping=sparql_escape_uri(mapping),
        manual_mapping_graph=sparql_escape_uri(MANUAL_MAPPING_GRAPH),
        related_mappings=",\n            ".join([sparql_escape_uri(m) for m in related_mappings])
    )
    query_result = query(query_string)
    if query_result["results"]["bindings"]:
        return to_recs(query_result)
    else:
        return []


def write_cluster(members, created=datetime.datetime.now()):
    CLUSTER_BASE_URI = "http://data.publiq.be/cluster/"
    uuid = generate_uuid()
    uri = CLUSTER_BASE_URI + uuid
    query_template = Template("""
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT DATA {
    GRAPH $graph {
        $cluster a ext:Cluster ;
            mu:uuid $uuid ;
            dct:created $created ;
            dct:modified $modified ;
            ext:member $members .
    }
}

""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(CLUSTER_GRAPH),
        uuid=sparql_escape_string(uuid),
        cluster=sparql_escape_uri(uri),
        created=sparql_escape_datetime(created),
        modified=sparql_escape_datetime(created),
        members=", ".join([sparql_escape_uri(m) for m in members])
    )
    update(query_string)

def query_verified_location_mapping_by_address_mapping(address_mapping):
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX locn: <http://www.w3.org/ns/locn#>
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT (?verified_location_mapping AS ?uri) ?a ?a_label ?b ?b_label
WHERE {
    GRAPH $mapping_graph {
        $address_mapping a sssom:Mapping .
        {
            $address_mapping sssom:subject_id ?address .
        }
        UNION
        {
            $address_mapping sssom:object_id ?address .
        }
    }
    VALUES ?g {
        <http://locatieslinkeddata.ticketgang-locations.ticketing.acagroup.be>
        <http://locatiessparql.kunstenpunt-locaties.professionelekunsten.kunsten.be>
        <http://placessparql.publiq-uit-locaties.vrijetijdsparticipatie.publiq.be>
        <http://organisatorensparql.publiq-uit-organisatoren.vrijetijdsparticipatie.publiq.be>
    }
    GRAPH ?g {
        ?location locn:address ?address .
    }
    GRAPH $manual_mapping_graph {
        ?verified_location_mapping a sssom:Mapping ;
            sssom:subject_id ?a ;
            sssom:subject_label ?a_label ;
            sssom:object_id ?b ;
            sssom:object_label ?b_label ;
            sssom:mapping_justification <https://w3id.org/semapv/vocab/ManualMappingCuration> ;
            sssom:predicate_id ?predicate_id .
        VALUES ?predicate_id {
            <http://www.w3.org/2004/02/skos/core#exactMatch>
            <http://www.w3.org/2004/02/skos/core#relatedMatch>
            # 'http://mu.semte.ch/vocabularies/ext/noMatch'
        }
        {
            ?verified_location_mapping sssom:subject_id ?location .
        }
        UNION
        {
            ?verified_location_mapping sssom:object_id ?location .
        }
    }
}
""")
    query_string = query_template.substitute(
        address_mapping=sparql_escape_uri(address_mapping),
        mapping_graph=sparql_escape_uri(MAPPING_GRAPH),
        manual_mapping_graph=sparql_escape_uri(MANUAL_MAPPING_GRAPH),
    )
    query_result = query(query_string)
    if query_result["results"]["bindings"]:
        return to_recs(query_result)
    else:
        return []

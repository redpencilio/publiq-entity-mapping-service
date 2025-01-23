from string import Template
import datetime

from helpers import query, update, generate_uuid
from escape_helpers import (sparql_escape_string,
    sparql_escape_uri,
    sparql_escape_datetime,
    sparql_escape_float)
from query_result_helpers import to_recs

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

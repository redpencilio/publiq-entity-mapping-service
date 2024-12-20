from string import Template
from helpers import query, update, generate_uuid
from escape_helpers import (sparql_escape_string,
    sparql_escape_uri,
    sparql_escape_datetime,
    sparql_escape_float)
from query_result_helpers import to_recs
from address import Address

MAPPING_GRAPH = "http://mu.semte.ch/graphs/entity-mappings"

def write_mapping(a, b, created, score):
    MAPPING_BASE_URI = "http://data.publiq.be/mappings/"
    uuid = generate_uuid()
    uri = MAPPING_BASE_URI + uuid
    query_template = Template("""
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>

INSERT DATA {
    GRAPH $graph {
        $mapping a sssom:Mapping ;
            dct:created $created ;
            dct:modified $modified ;
            sssom:subject_id $left_entity ;
            sssom:object_id $right_entity ;
            sssom:mapping_justification $mapping_justification ;
            sssom:similarity_score $similarity_score .
    }
}

""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(MAPPING_GRAPH),
        mapping=sparql_escape_uri(uri),
        created=sparql_escape_datetime(created),
        modified=sparql_escape_datetime(created),
        left_entity=sparql_escape_uri(a),
        right_entity=sparql_escape_uri(b),
        mapping_justification=sparql_escape_uri("https://w3id.org/semapv/vocab/CompositeMatching"),
        similarity_score=sparql_escape_float(score)
    )
    update(query_string)

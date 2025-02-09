from string import Template
from helpers import query, update, generate_uuid
from escape_helpers import (sparql_escape_string,
    sparql_escape_uri,
    sparql_escape_datetime,
    sparql_escape_float)
from query_result_helpers import to_recs
from address import Address

MAPPING_GRAPH = "http://mu.semte.ch/graphs/entity-mappings"

def load_address_mapping_page(page=0, size=50, _from=None):
    offset = page * size
    limit = size
    if _from:
        from_filter = f"FILTER (?created > {sparql_escape_datetime(_from)})"
    else:
        from_filter = ""
    query_template = Template("""
PREFIX locn: <http://www.w3.org/ns/locn#>
PREFIX sssom: <https://w3id.org/sssom/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT (?mapping AS ?uri) ?a_location ?b_location ?a_locator_name ?b_locator_name ?address_similarity_score
WHERE {
    GRAPH $graph {
        ?mapping
            a sssom:Mapping ;
            dct:created ?created ;
            sssom:subject_id ?a ;
            sssom:object_id ?b ;
            sssom:similarity_score ?address_similarity_score .
        $from_filter
    }
    ?a a locn:Address .
    ?b a locn:Address .
    ?a_location locn:address  ?a .
    ?b_location locn:address  ?b .
    FILTER (?a_location != ?b_location)
    ?a_location (locn:LocatorName | locn:locatorName) ?a_locator_name  .
    ?b_location (locn:LocatorName | locn:locatorName) ?b_locator_name  .
    FILTER NOT EXISTS {
        {
            ?existing_mapping
                sssom:subject_id ?a_location ;
                sssom:object_id ?b_location .
        }
        UNION
        {
            ?existing_mapping
                sssom:subject_id ?b_location ;
                sssom:object_id ?a_location .
        }
    }
}
ORDER BY DESC(?address_similarity_score)
OFFSET $offset
LIMIT $limit
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(MAPPING_GRAPH),
        from_filter=from_filter,
        offset=offset,
        limit=limit
    )
    query_result = query(query_string)
    if query_result["results"]["bindings"]:
        return to_recs(query_result)
    else:
        return None

from string import Template
from helpers import query
from query_result_helpers import to_recs
from address import Address

def load_addresses_page(page=0, size=50):
    offset = page * size
    limit = size
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX locn: <http://www.w3.org/ns/locn#>

SELECT (?address AS ?uri) ?full_address ?adminunitl1 ?postcode ?postname ?thoroughfare ?locator_designator
WHERE {
    {
        SELECT DISTINCT ?address ?full_address ?adminunitl1 ?postcode ?postname ?thoroughfare ?locator_designator
        WHERE {
            VALUES ?g {
                <http://locatieslinkeddata.ticketgang-locations.ticketing.acagroup.be>
                <http://locatiessparql.kunstenpunt-locaties.professionelekunsten.kunsten.be>
                <http://metadata.cultuurparticipatie-metadata.vrijetijdsparticipatie.publiq.be>
                <http://placessparql.publiq-uit-locaties.vrijetijdsparticipatie.publiq.be>
                <http://organisatorensparql.publiq-uit-organisatoren.vrijetijdsparticipatie.publiq.be>
            }
            GRAPH ?g {
                ?address a locn:Address .
                ?address locn:fullAddress ?full_address FILTER(LANG(?full_address) = "nl" ).
                OPTIONAL { ?address locn:postcode ?postcode . }
                ?address locn:postName ?postname FILTER(LANG(?postname) = "nl" ).
                OPTIONAL { ?address locn:thoroughfare ?thoroughfare FILTER(LANG(?thoroughfare) = "nl" ). }
                OPTIONAL { ?address locn:locatorDesignator ?locator_designator . }
                ?address locn:adminUnitL1 ?adminunitl1 .
            }
            FILTER NOT EXISTS { ?address ^locn:address/prov:invalidatedAtTime ?time . }
        }
        ORDER BY ?postcode
    }
}
OFFSET $offset
LIMIT $limit
""")
    query_string = query_template.substitute(
        offset=offset,
        limit=limit
    )
    query_result = query(query_string)
    if query_result["results"]["bindings"]:
        return [Address(**a) for a in to_recs(query_result)]
    else:
        return None

def load_addresses():
    addresses = []
    page = 0
    while True:
        addresses_page = load_addresses_page(page)
        if addresses_page:
            addresses = addresses + addresses_page
            page += 1
            ### tmp
            if len(addresses) > 100:
                break
        else:
            break
    return addresses

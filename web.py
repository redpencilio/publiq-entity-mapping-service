from datetime import datetime

from thefuzz import process, fuzz

from flask import request
from helpers import logger, query, update

from skolemization import object_query_template, subject_query_template

from load_addresses import load_addresses
from load_address_mappings import load_address_mapping_page
from load_location_mappings import query_related_mappings, load_ungrouped_location_mapping, write_cluster
from mapping import write_mapping, find_mapping_for_uris

def map_addresses(a, b):
    score = a.score(b)
    if score > 50:
        logger.debug(f"-------\nMatch for \n{a.full_address}")
        logger.debug(f"{str(b)} full score {a.score(b)}")
        existing_mapping = find_mapping_for_uris(a.uri, b.uri)  # by URI
        if existing_mapping:
            logger.info(f"Match already has been recorded previously. Full score was {existing_mapping['similarity_score']}. Skipping")
        else:
            logger.info(f"Writing match to DB")
            write_mapping(a.uri,
                          a.full_address,
                          b.uri,
                          b.full_address,
                          datetime.now(),
                          score)

@app.route("/map-addresses")
def map_all_addresses():
    """
    Load all addresses,
    Calculate mappings between them,
    Write mappings that aren't in the DB yet.
    """
    _from = request.args.get("from")
    if _from:
        from_date = datetime.fromisoformat(_from)
    else:
        from_date = None

    all_addresses = load_addresses(_from=None)
    if not from_date:
        to_map_addresses = all_addresses
    else:
        to_map_addresses = load_addresses(_from=from_date)

    addresses_by_full = {a.full_address: a for a in all_addresses}
    full_addresses_list = addresses_by_full.keys()
    for address in to_map_addresses:
        bestmatches = process.extract(address.full_address, full_addresses_list, limit=20)
        qualifying = [bestmatch for bestmatch in bestmatches[1:] if bestmatch[1] > 91 ]
        if qualifying:
            for potential_match in qualifying:
                class_match = addresses_by_full[potential_match[0]]
                map_addresses(address, class_match)

@app.route("/map-locations-by-address")
def map_locations_by_address():
    page = 0
    while True:
        address_mappings_page = load_address_mapping_page(page)
        if address_mappings_page:
            for address_mapping in address_mappings_page:
                a_locator_name, b_locator_name = address_mapping["a_locator_name"], address_mapping["b_locator_name"]
                score = fuzz.WRatio(a_locator_name, b_locator_name)
                if score > 50:
                    a_uri, b_uri = address_mapping["a_location"], address_mapping["b_location"]
                    logger.debug(f"match for {a_locator_name} VS {b_locator_name}, {score}")
                    existing_mapping = find_mapping_for_uris(a_uri, b_uri)
                    if existing_mapping:
                        logger.info(f"Match already has been recorded previously. Full score was {address_mapping['address_similarity_score']}. Skipping")
                    else:
                        logger.info(f"Writing match to DB")
                        write_mapping(a_uri,
                                      a_locator_name,
                                      b_uri,
                                      b_locator_name,
                                      datetime.now(),
                                      score)
            page += 1
        else:
            break

def load_related_mappings(mapping, traversed_mappings=[]):
    if not traversed_mappings:
        traversed_mappings = [mapping]

    results = query_related_mappings(mapping, traversed_mappings)
    related_mappings = [r["related_mapping"] for r in results]
    traversed_mappings = traversed_mappings + related_mappings
    print(f"loading round, found {len(results)}")
    new_related_mappings = []
    for mapping in related_mappings:
        new_related_mappings = new_related_mappings + load_related_mappings(mapping, traversed_mappings)
    return related_mappings + new_related_mappings


@app.route("/cluster-location-mappings")
def cluster_location_mappings():
    i = 0
    while True:
        res = load_ungrouped_location_mapping()
        if not res:
            break
        else:
            i += 1
        mapping = res['mapping']
        logger.info(f"Clustering mappings for mapping {mapping}")
        related_mappings = load_related_mappings(res["mapping"])
        if len(related_mappings) > 0:
            logger.info(f"Found {len(related_mappings)} related mappings. Writing to cluster")
            # Fixme: Relate locations instead? Having the source mappings is useful too?
            related_mappings.append(mapping)
            write_cluster(related_mappings)
        else:
            logger.debug(f"Found no related mappings")
            write_cluster([mapping])

    return f"{i} clusters created"

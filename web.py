from datetime import datetime

from thefuzz import process, fuzz

from helpers import logger

from load_addresses import load_addresses
from load_address_mappings import load_address_mapping_page
from mapping import write_mapping, check_mapping_existence

@app.route("/full-address-mapping")
def map_all_addresses():
    """
    Load all addresses,
    Calculate mappings between them,
    Write mappings that aren't in the DB yet.
    """
    addresses = load_addresses()

    addresses_by_full = {a.full_address: a for a in addresses}
    full_addresses_list = addresses_by_full.keys()
    for address in addresses:
        bestmatches = process.extract(address.full_address, full_addresses_list, limit=20)
        qualifying = [bestmatch for bestmatch in bestmatches[1:] if bestmatch[1] > 91 ]
        if qualifying:
            for potential_match in qualifying:
                class_match = addresses_by_full[potential_match[0]]
                score = address.score(class_match)
                if score > 50:
                    logger.debug(f"-------\nMatch for \n{address.full_address}")
                    logger.debug(f"{str(potential_match[0])} full score {address.score(class_match)}")
                    existing_mapping = check_mapping_existence(address.uri, class_match.uri)
                    if existing_mapping:
                        logger.info(f"Match already has been recorded previously. Full score was {existing_mapping['similarity_score']}. Skipping")
                    else:
                        logger.info(f"Writing match to DB")
                        write_mapping(address.uri,
                                    address.full_address,
                                    class_match.uri,
                                    class_match.full_address,
                                    datetime.now(),
                                    score)

@app.route("/map-locations-by-address")
def map_locations_by_address():
    """
    Load address mappings,
    Calculate mappings between associated locations,
    Write mappings that aren't in the DB yet.
    """
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
                    existing_mapping = check_mapping_existence(a_uri, b_uri)
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


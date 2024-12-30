from datetime import datetime

from thefuzz import process

from helpers import logger

from load_addresses import load_addresses
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

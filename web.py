from load_addresses import load_addresses
from mapping import write_mapping
from thefuzz import process

@app.route("/hello")
def hello():
    addresses = load_addresses()

    addresses_by_full = {a.full_address: a for a in addresses}
    full_addresses_list = addresses_by_full.keys()
    for address in addresses:
        bestmatches = process.extract(address.full_address, full_addresses_list, limit=20)
        qualifying = [bestmatch for bestmatch in bestmatches[1:] if bestmatch[1] > 91 ]
        if qualifying:
            # print(f"Matches for {address.full_address}")
            for potential_match in qualifying:
                class_match = addresses_by_full[potential_match[0]]
                score = address.score(class_match)
                if score > 89:
                    print(f"-------\nMatch for \n{address.full_address}")
                    print(f"{str(potential_match[0])} full score {address.score(class_match)}")
                    write_mapping(address.uri, class_match.uri, datetime.now(), score)

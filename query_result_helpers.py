import collections
from typing import DefaultDict, Dict, List

def to_recs(result: Dict) -> List[DefaultDict]:
    bindings = result["results"]["bindings"]
    return [
        collections.defaultdict(
            lambda: None,
            [(k, v["value"]) for k, v in b.items()]
        )
        for b in bindings]

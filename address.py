from __future__ import annotations
import typing
import re
from dataclasses import dataclass

from thefuzz import fuzz

be_housenum_re = re.compile(r"(?P<num>(\d+)) *[-\/]?(?P<let>([a-zA-Z]{1})?)")

@dataclass
class Address:
    uri: str
    full_address: str
    adminunitl1: str
    postcode: typing.Optional[str] = None
    postname: typing.Optional[str] = None
    thoroughfare: typing.Optional[str] = None
    locator_designator: typing.Optional[str] = None

    @property
    def specificity(self):
        specificty = 0
        MAX_SPECIFICITY = 5
        if self.adminunitl1:
            specificty += 1
        if self.postcode:
            specificty += 1
        if self.postname:
            specificty += 1
        if self.thoroughfare:
            specificty += 1
        if self.locator_designator:
            specificty += 1
        normalized_specificity = specificty / MAX_SPECIFICITY * 100
        # print(f"score specificiteit {normalized_specificity}")
        return normalized_specificity

    def score_specificity(self, b: typing.Self):
        return 100 - abs(self.specificity - b.specificity)

    def score_adminunitl1(self, b: typing.Self):
        return fuzz.partial_token_set_ratio(self.adminunitl1, b.adminunitl1)

    def score_postcode(self, b: typing.Self):
        try:
            return 100 - abs(int(self.postcode) - int(b.postcode))
        except Exception:
            return 0

    def score_postname(self, b: typing.Self):
        return fuzz.partial_token_set_ratio(self.postname, b.postname)

    def score_thoroughfare(self, b: typing.Self):
        return fuzz.partial_token_set_ratio(self.thoroughfare, b.thoroughfare)

    def score_locator_designator(self, b: typing.Self):
        if self.locator_designator and b.locator_designator:
            match = be_housenum_re.match(self.locator_designator)
            match_b = be_housenum_re.match(b.locator_designator)
            if match and match_b:
                num = int(match["num"]) if "num" in match.groupdict() else None
                num_b = int(match_b["num"]) if "num" in match_b.groupdict() else None
                score_num = max(100 - abs(num - num_b), 0)
                if score_num == 100:
                    let = match["let"] if "let" in match.groupdict() else None
                    # print(f'{num}, {let}')
                    let_b = match_b["let"] if "let" in match_b.groupdict() else None
                    # print(f'{num_b}, {let_b}')
                    if let is None and let_b is None:
                        return score_num
                    elif let and let_b:
                        let_num = ord(let.lower()) - 96
                        let_num_b = ord(let_b.lower()) - 96
                        score_let = max(100 - abs(let_num - let_num_b), 0)
                        return (score_let + score_num) / 2
                    else:
                        logger.debug(f"Only one of both locator designators has a letter {match} {match_b}")
                        return 95
                else:
                    return score_num
            else:
                return 0
        elif not self.locator_designator and not b.locator_designator:
            return 100
        else:
            # unless for the case 1 vs none?
            return 0

    def score(self, b: typing.Self):
        scoring_funs = [
            (self.score_specificity, 1),
            (self.score_adminunitl1, 1),
            (self.score_postcode, 0.7),
            (self.score_postname, 0.7),
            (self.score_thoroughfare, 1),
            (self.score_locator_designator, 1)
        ]

        score = 0
        used_scores = 0
        for scoring_fun, weight in scoring_funs:
            res = scoring_fun(b)
            if res:
                score += res * weight
                used_scores += 1
        max_score = used_scores * 100
        normalized_score = score / max_score * 100
        return normalized_score

    def __str__(self):
        return self.full_address

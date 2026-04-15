from __future__ import annotations

import re
from typing import Dict


CITY_TO_ICB_FRAGMENT: Dict[str, str] = {
    "stoke on trent": "staffordshire and stoke-on-trent",
    "stoke-on-trent": "staffordshire and stoke-on-trent",
    "leeds": "west yorkshire",
    "bradford": "west yorkshire",
    "wakefield": "west yorkshire",
    "huddersfield": "west yorkshire",
    "halifax": "west yorkshire",
    "liverpool": "cheshire and merseyside",
    "newcastle": "north east and north cumbria",
    "sunderland": "north east and north cumbria",
    "gateshead": "north east and north cumbria",
    "middlesbrough": "north east and north cumbria",
    "sheffield": "south yorkshire",
    "doncaster": "south yorkshire",
    "barnsley": "south yorkshire",
    "rotherham": "south yorkshire",
    "preston": "lancashire",
    "blackpool": "lancashire",
    "blackburn": "lancashire",
    "burnley": "lancashire",
    "bolton": "greater manchester",
    "oldham": "greater manchester",
    "rochdale": "greater manchester",
    "salford": "greater manchester",
    "stockport": "greater manchester",
    "tameside": "greater manchester",
    "trafford": "greater manchester",
    "wigan": "greater manchester",
    "bury": "greater manchester",
    "barking": "north east london",
    "dagenham": "north east london",
    "hackney": "north east london",
    "tower hamlets": "north east london",
    "newham": "north east london",
    "waltham forest": "north east london",
    "redbridge": "north east london",
    "havering": "north east london",
    "camden": "north central london",
    "islington": "north central london",
    "barnet": "north central london",
    "enfield": "north central london",
    "haringey": "north central london",
    "greenwich": "south east london",
    "lewisham": "south east london",
    "bromley": "south east london",
    "bexley": "south east london",
    "lambeth": "south east london",
    "southwark": "south east london",
    "wandsworth": "south west london",
    "richmond": "south west london",
    "kingston": "south west london",
    "croydon": "south west london",
    "merton": "south west london",
    "sutton": "south west london",
    "ealing": "north west london",
    "hounslow": "north west london",
    "hillingdon": "north west london",
    "brent": "north west london",
    "harrow": "north west london",
    "hammersmith": "north west london",
    "brighton": "sussex",
    "eastbourne": "sussex",
    "worthing": "sussex",
    "hastings": "sussex",
    "reading": "buckinghamshire",
    "slough": "frimley",
    "oxford": "buckinghamshire",
    "norwich": "norfolk",
    "ipswich": "suffolk",
    "cambridge": "cambridgeshire",
    "gloucester": "gloucestershire",
    "cheltenham": "gloucestershire",
    "exeter": "devon",
    "plymouth": "devon",
    "torbay": "devon",
    "york": "humber and north yorkshire",
    "scarborough": "humber and north yorkshire",
    "hull": "humber and north yorkshire",
    "grimsby": "north east lincolnshire",
    "scunthorpe": "north lincolnshire",
    "lincoln": "lincolnshire",
    "wolverhampton": "black country",
    "dudley": "black country",
    "sandwell": "black country",
    "walsall": "black country",
    "worcester": "herefordshire and worcestershire",
    "hereford": "herefordshire and worcestershire",
    "shrewsbury": "shropshire",
    "telford": "shropshire",
    "portsmouth": "hampshire",
    "southampton": "hampshire",
    "winchester": "hampshire",
    "canterbury": "kent",
    "maidstone": "kent",
    "medway": "kent",
    "bath": "bath",
    "taunton": "somerset",
    "swindon": "bath",
    "bristol": "bristol",
    "peterborough": "cambridgeshire",
    "luton": "bedfordshire",
    "watford": "hertfordshire",
    "stevenage": "hertfordshire",
    "chelmsford": "mid and south essex",
    "southend": "mid and south essex",
    "colchester": "north east essex",
    "basildon": "mid and south essex",
}


def normalize_alias_text(text: str) -> str:
    return re.sub(r"[\s\-]+", " ", (text or "").strip().lower())


def city_to_icb_name(text: str) -> str:
    fragment = CITY_TO_ICB_FRAGMENT.get(normalize_alias_text(text), "")
    if not fragment:
        return ""
    return f"NHS {fragment.title()} Integrated Care Board"


def find_city_icb_in_text(text: str) -> str:
    normalized = normalize_alias_text(text)
    for city in sorted(CITY_TO_ICB_FRAGMENT, key=len, reverse=True):
        if re.search(rf"\b{re.escape(normalize_alias_text(city))}\b", normalized):
            return city_to_icb_name(city)
    return ""

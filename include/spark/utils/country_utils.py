import re
from typing import Optional

import pandas as pd
import pycountry
from flashgeotext.geotext import GeoText
from pyspark import Broadcast
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType

_geotext = None

SUBDIVISION_TO_COUNTRY = {
    sub.name.lower(): pycountry.countries.get(alpha_2=sub.country_code).name
    for sub in pycountry.subdivisions
    if pycountry.countries.get(alpha_2=sub.country_code)
}


def clean_location_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[()\[\],]", " ", str(text))
    return " ".join(text.split())


def get_country_from_text(loc: str, sub_dict: dict) -> Optional[str]:
    global _geotext
    if _geotext is None:
        _geotext = GeoText()

    if not loc or str(loc).lower() in ["null", "none", "nan", ""]:
        return None

    cleaned_loc = clean_location_text(loc)

    text_lower = cleaned_loc.lower()
    words = set(text_lower.split())
    for sub_name, country_name in sub_dict.items():
        if sub_name in words:
            return country_name

    return None


def get_extract_country_udf(subdivision_bc: Broadcast[dict]) -> callable:

    @pandas_udf(StringType())
    def extract_country_udf(locations: pd.Series) -> pd.Series:

        sub_dict = subdivision_bc.value
        return locations.apply(lambda loc: get_country_from_text(loc, sub_dict))

    return extract_country_udf

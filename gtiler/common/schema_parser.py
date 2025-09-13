import yaml
from typing import List

from constants import GediProduct


def check_schema(schema_path: str) -> None:
    with open(schema_path, "r") as f:
        schema = yaml.safe_load(f)
        for p in schema.keys():
            if p == "derived":
                continue
            if p not in ["level2A", "level2B", "level4A", "level4C"]:
                raise ValueError(f"Schema file contains unknown product {p}.")
            elif "variables" not in schema[p]:
                raise ValueError(
                    f"Schema file must contain 'variables' for {p}."
                )
            elif "shot_number" not in schema[p]["variables"].keys():
                raise ValueError(
                    f"{p} schema must contain 'shot_number' in all products."
                )
            elif not any(
                "lat_lowestmode" in v.lower()
                for v in schema[p]["variables"].keys()
            ):
                raise ValueError(
                    f"{p} schema must contain 'lat_lowestmode' variable."
                )
            elif not any(
                "lon_lowestmode" in v.lower()
                for v in schema[p]["variables"].keys()
            ):
                raise ValueError(
                    f"{p} schema must contain 'lon_lowestmode' variable."
                )
            col_names = [schema[p]["variables"].keys() for p in schema.keys()]
            col_names = [
                item
                for sublist in col_names
                for item in sublist
                if item
                not in ["shot_number", "lat_lowestmode", "lon_lowestmode"]
            ]
            if len(set(col_names)) != len(col_names):
                # find the duplicates
                dups = [x for x in col_names if col_names.count(x) > 1]
                raise ValueError(
                    f"Column names must be unique across products, but found duplicates: {dups}."
                )
        return schema


def get_products(schema) -> List[GediProduct]:
    products = []
    for p in schema.keys():
        if p == "derived":
            continue
        if p == "level2A":
            products.append(GediProduct.L2A)
        elif p == "level2B":
            products.append(GediProduct.L2B)
        elif p == "level4A":
            products.append(GediProduct.L4A)
        elif p == "level4C":
            products.append(GediProduct.L4C)
    return products

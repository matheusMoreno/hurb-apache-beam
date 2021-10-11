#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Module with auxiliary functions for the Covid aggregator.

Hurb Data Engineer Challenge
Author: Matheus Fernandes Moreno
"""

import csv
from typing import Any, Iterator, Dict


def convert_ints_from_dict(dictionary: Dict) -> Dict:
    """Convert integer values from a dictionary."""
    for key, value in dictionary.items():
        try:
            dictionary[key] = int(value)
        except (TypeError, ValueError):
            continue
    return dictionary


def import_csv_rows(filepath: str, datatype: Any = dict) -> Iterator[Any]:
    """Generate data from a CSV file.

    For simplicity, the function considers that the CSV file has minimal
    quoting and ';' as the delimiter.

    Parameters
    ----------
    filepath : str
        Path of the CSV file.
    datatype : Any, optional
        Type of the returned data. Must be a type/function.

    Yields
    -------
    Iterator[Any]
        The rows of the CSV, as the desired datatype/transform.
    """
    with open(filepath, mode='r', encoding='utf-8') as filedesc:
        reader = csv.DictReader(
            filedesc, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            row = {
                ''.join(c for c in k if c.isalpha()): v
                for k, v in row.items()
            }
            row = convert_ints_from_dict(row)
            yield datatype(**dict(row))


def import_states_data_by_key(filepath: str, key: str) -> Dict:
    """Generate a dict from a CSV, maped by the column `key`."""
    return {
        data[key]: data
        for data in import_csv_rows(filepath)
    }

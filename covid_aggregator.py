#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Apache Beam pipeline that aggregates brazilian Covid data.

Hurb Data Engineer Challenge
Author: Matheus Fernandes Moreno
"""

# Ideias de pipeline:
#   - Importar o arquivo de Casos
#   - Usar um Filter() no csv Casos para remover sem estado e com município
#     (sobrando assim apenas dados gerais dos estados)
#   - Agregar o casosAcumulado e obitosAcumulado por coduf em Casos
#   - Recuperar as informações do estado a partir do coduf de Casos
#   - Excluir e renomear colunas para gerar
#   - Gerar o CSV
#   - Gerar o json a partir do CSV (muito fácil)

import json
import logging
from typing import Iterable, NamedTuple, Dict, TypedDict
from collections import OrderedDict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import import_states_data_by_key, import_csv_rows


LOGGER = logging.getLogger(__name__)

INPUT_FILEPATH = 'data/HIST_PAINEL_COVIDBR_28set2020.csv'
OUTPUTS_FILEPATH_PREFIX = 'outputs/states_data'

OUTPUTS_INPUT_FIELD_MAPPINGS = OrderedDict({
    'Regiao': 'regiao',
    'Estado': 'UF',
    'UF': 'estado',
    'Governador': 'Governador',
    'TotalCasos': 'TotalCasos',
    'TotalObitos': 'TotalObitos',
})

STATES_DATA_FILEPATH = 'data/EstadosIBGE.csv'
STATES_DATA_BY_CODE = import_states_data_by_key(STATES_DATA_FILEPATH, 'Código')


def check_if_state_data(row: TypedDict) -> bool:
    """Check if a row corresponds to data from a state."""
    return row.estado and not row.municipio


def extend_state_data_by_code(data: NamedTuple) -> Dict:
    """Add metadata for state based on its code."""
    return {**data._asdict(), **STATES_DATA_BY_CODE[data.coduf]}


def filter_and_rename_fields(data: Dict) -> OrderedDict:
    """Get only relevant fields of the data."""
    return OrderedDict({
        new_field: data.get(old_field)
        for new_field, old_field in OUTPUTS_INPUT_FIELD_MAPPINGS.items()
    })


def format_output_csv(rows: Iterable[OrderedDict]) -> str:
    """Create a string of the resulting CSV file.

    The Combine logic must be commutative and associative; therefore,
    the function must consider the possibility that some elements of `rows`
    are already formatted, i.e., are strings.
    """
    return '\n'.join(
        row if isinstance(row, str) else
        ';'.join(str(e) for e in row.values())
        for row in rows
    )


def format_output_json(rows: Iterable[OrderedDict]) -> str:
    """Create a string of the resulting JSON file.

    The Combine logic must be commutative and associative; therefore,
    the function must consider the possibility that some elements of `rows`
    are already formatted, i.e., are strings.
    """
    dict_rows = [
        elem for row in rows
        for elem in (json.loads(row) if isinstance(row, str) else [row])
    ]
    return json.dumps(list(dict_rows), ensure_ascii=False, indent=4)


def main():
    """Execute the pipeline."""
    options = PipelineOptions(flags=[], type_check_additional='all')
    input_filepath = INPUT_FILEPATH
    rows = import_csv_rows(input_filepath, datatype=beam.Row)

    with beam.Pipeline(options=options) as pipeline:
        # Main processing pipeline: compute and format data
        processed_data = (
            pipeline
            | 'Create collection from CSV file' >> beam.Create(rows)
            | 'Keep only data from states' >> beam.Filter(check_if_state_data)
            | 'Aggregate values' >> beam.GroupBy('coduf', 'regiao', 'estado')
                .aggregate_field('casosNovos', sum, 'TotalCasos')       # noqa
                .aggregate_field('obitosNovos', sum, 'TotalObitos')
            | 'Add states metadata' >> beam.Map(extend_state_data_by_code)
            | 'Filter and rename fields' >> beam.Map(filter_and_rename_fields)
        )

        # First output pipeline: write the .csv file
        _ = (
            processed_data
            | 'Format CSV output' >> beam.CombineGlobally(format_output_csv)
            | 'Write to CSV file' >> beam.io.WriteToText(
                OUTPUTS_FILEPATH_PREFIX,
                file_name_suffix='.csv',
                shard_name_template='',
                header=';'.join(OUTPUTS_INPUT_FIELD_MAPPINGS.keys())
            )
        )

        # Second output pipeline: write the .json file
        _ = (
            processed_data
            | 'Format JSON output' >> beam.CombineGlobally(format_output_json)
            | 'Write to JSON file' >> beam.io.WriteToText(
                OUTPUTS_FILEPATH_PREFIX,
                file_name_suffix='.json',
                shard_name_template=''
            )
        )


if __name__ == '__main__':
    # TODO: log stuff
    LOGGER.setLevel(logging.INFO)
    main()

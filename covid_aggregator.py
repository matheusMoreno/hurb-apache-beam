#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Apache Beam pipeline that aggregates brazilian Covid data.

Hurb Data Engineer Challenge
Author: Matheus Fernandes Moreno
"""

import json
import argparse
import logging
from typing import Iterable, NamedTuple, Dict, Tuple
from collections import OrderedDict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import import_states_data_by_key, import_csv_rows


LOGGER = logging.getLogger(__name__)

OUTPUTS_INPUT_FIELD_MAPPINGS = OrderedDict({
    'Regiao': 'regiao',
    'Estado': 'UF',
    'UF': 'estado',
    'Governador': 'Governador',
    'TotalCasos': 'TotalCasos',
    'TotalObitos': 'TotalObitos',
})


def retrieve_args() -> Tuple[argparse.Namespace, list]:
    """Parse args used by the script and by Apache Beam."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', '--input_file',
        default='data/HIST_PAINEL_COVIDBR_28set2020.csv',
        help='Filepath of the input CSV.'
    )
    parser.add_argument(
        '-o', '--outputs_prefix',
        default='outputs/states_data',
        help='Prefix of the output files.'
    )
    parser.add_argument(
        '-s', '--states_file',
        default='data/EstadosIBGE.csv',
        help='Filepath with metadata for the brazilian states.'
    )

    main_args, beam_options = parser.parse_known_args()
    LOGGER.debug("Found local arguments: %s.", vars(main_args))
    return main_args, beam_options


def check_if_state_data(row) -> bool:
    """Check if a row corresponds to data from a state."""
    return row.estado and not row.municipio


def extend_state_data_by_code(data: NamedTuple, states_data=None) -> Dict:
    """Add metadata for state based on its code."""
    states_data = {} if states_data is None else states_data
    return {**data._asdict(), **states_data.get(data.coduf, {})}


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
    LOGGER.debug("Formatting rows: %s", list(rows))
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
    LOGGER.debug("Formatting rows: %s", list(rows))
    dict_rows = [
        elem for row in rows
        for elem in (json.loads(row) if isinstance(row, str) else [row])
    ]
    return json.dumps(list(dict_rows), ensure_ascii=False, indent=4)


def main():
    """Execute the pipeline."""
    main_args, beam_options = retrieve_args()
    options = PipelineOptions(beam_options, type_check_additional='all')

    # Instantiate iterable with rows and states data
    csv_rows = import_csv_rows(main_args.input_file, datatype=beam.Row)
    states_data = import_states_data_by_key(main_args.states_file, 'Código')

    LOGGER.info(
        "Starting the Beam pipeline with options:\n%s",
        '\n'.join(f"\t{k}=={v}" for k, v in vars(main_args).items())
    )

    with beam.Pipeline(options=options) as pipeline:
        # Main processing pipeline: compute and format data
        processed_data = (
            pipeline
            | 'Create collection from CSV file' >> beam.Create(csv_rows)
            | 'Keep only data from states' >> beam.Filter(check_if_state_data)
            | 'Aggregate values' >> beam.GroupBy('coduf', 'regiao', 'estado')
                .aggregate_field('casosNovos', sum, 'TotalCasos')       # noqa
                .aggregate_field('obitosNovos', sum, 'TotalObitos')
            | 'Add states metadata' >> beam.Map(
                extend_state_data_by_code, states_data=states_data)
            | 'Filter and rename fields' >> beam.Map(filter_and_rename_fields)
        )

        # First output pipeline: write the .csv file
        _ = (
            processed_data
            | 'Format CSV string' >> beam.CombineGlobally(format_output_csv)
            | 'Write to CSV output file' >> beam.io.WriteToText(
                main_args.outputs_prefix,
                file_name_suffix='.csv',
                shard_name_template='',
                header=';'.join(OUTPUTS_INPUT_FIELD_MAPPINGS.keys())
            )
        )

        # Second output pipeline: write the .json file
        _ = (
            processed_data
            | 'Format JSON string' >> beam.CombineGlobally(format_output_json)
            | 'Write to JSON output file' >> beam.io.WriteToText(
                main_args.outputs_prefix,
                file_name_suffix='.json',
                shard_name_template=''
            )
        )

    LOGGER.info("Done. Outputs in %s*.", main_args.outputs_prefix)


if __name__ == '__main__':
    # Set logging levels. The level of apache_beam.coders is raised to suppress
    # a warning regarding the use of an implied deterministic coder. For this
    # simple implementation, we can just ignore it. For more information, see
    # https://stackoverflow.com/questions/63219092/
    LOGGER.setLevel(logging.INFO)
    logging.getLogger('apache_beam.coders').setLevel(logging.ERROR)

    # Run the main function
    main()

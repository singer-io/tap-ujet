#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_ujet.client import UjetClient
from tap_ujet.discover import discover
from tap_ujet.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'company_key',
    'company_secret',
    'subdomain',
    'domain',
    'start_date',
    'user_agent'
]


def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with UjetClient(parsed_args.config['company_key'],
                    parsed_args.config['company_secret'],
                    parsed_args.config['subdomain'],
                    parsed_args.config['domain'],
                    parsed_args.config['user_agent']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 config=parsed_args.config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()

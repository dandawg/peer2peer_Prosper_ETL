# -*- coding: utf-8 -*-

import os, sys, logging
import listings_attributes as atts

def main():
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    tools_path = os.path.abspath(os.path.join(BASE_DIR, '../tools/'))

    sys.path.append(tools_path)
    import prosper_api_tools

    # logging setup
    logfile = os.path.abspath(os.path.join(BASE_DIR, '../logs/prosper_owned_listing_data_ETL.log'))
    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    logging.info('Starting prosper_owned_listing_data_ETL.py script...')

    # column schema
    #==============
    all_cols = set(atts.top_level_atributes)
    deprecated_cols = set(atts.top_level_deprecated)
    columns = list(all_cols - deprecated_cols)

    # get prosper connection tokens
    #==============================
    logging.info('initiating conn. to prosper...')
    access_response = prosper_api_tools.initiate_conn()
    tokens = access_response.json()
    logging.info('connection obtained')

    # get data and write to file
    #===========================
    file_dir = os.path.join(BASE_DIR, '../data/mylistings/')
    file_path = 'mylistings.bz2'
    full_path = os.path.join(file_dir, file_path)

    logging.info('initiating data pull...')
    prosper_api_tools.get_all_owned_listings(full_path, tokens, column_schema=columns)
    logging.info('done')

if __name__ == '__main__':
    main()
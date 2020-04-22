#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retrieves owned notes from Prosper API.
See https://developers.prosper.com/docs/investor/notes-api/
for API details.
"""

# imports
import os, sys, logging

def main():
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    tools_path = os.path.abspath(os.path.join(BASE_DIR, '../tools/'))

    sys.path.append(tools_path)
    import prosper_api_tools

    # logging setup
    LOGGING_LEVEL = logging.INFO
    logfile = os.path.abspath(os.path.join(BASE_DIR, '../logs/prosper_notes_ETL.log'))
    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=LOGGING_LEVEL
    )
    logging.info('Starting prosper_notes_ETL.py script...')

    # column schema
    #==============
    COLUMN_SCHEMA = [    
        "age_in_months",
        "amount_borrowed",
        "borrower_rate",
        "collection_fees_paid_pro_rata_share",
        "days_past_due",
        "debt_sale_proceeds_received_pro_rata_share",
    #    "group_leader_award",  # deprecated field
        "interest_paid_pro_rata_share",
        "is_sold",
        "late_fees_paid_pro_rata_share",
        "listing_number",
        "loan_note_id",
        "loan_number",
        "next_payment_due_amount_pro_rata_share",
        "next_payment_due_date",
        "note_default_reason",
        "note_default_reason_description",
        "note_ownership_amount",
        "note_sale_fees_paid",
        "note_sale_gross_amount_received",
        "note_status",
        "note_status_description",
        "origination_date",
        "principal_balance_pro_rata_share",
        "principal_paid_pro_rata_share",
        "prosper_fees_paid_pro_rata_share",
        "prosper_rating",
        "service_fees_paid_pro_rata_share",
        "term",
    ]
    # get prosper connection tokens
    #==============================
    logging.info('initiating conn. to prosper...')
    access_response = prosper_api_tools.initiate_conn()
    tokens = access_response.json()
    logging.info('connection obtained')

    # get data and write to file
    #===========================
    file_dir = os.path.join(BASE_DIR, '../data/mynotes/')
    file_path = 'mynotes.bz2'
    full_path = os.path.join(file_dir, file_path)

    #backup old file first
    if os.path.exists(full_path):
        logging.info(f'backing up old file {full_path} to {full_path + ".bak"}')
        os.rename(full_path, full_path + '.bak')
        logging.info('finished backing up')

    logging.info('initiating data pull...')
    prosper_api_tools.get_many_notes(full_path, tokens, column_schema=COLUMN_SCHEMA, timezn='America/Denver')
    logging.info('done')

if __name__ == '__main__':
    main()
#!/usr/bin/python

'''This is a script to get current loan data from Prosper on loans that I own.
(For now, the script will get all loans.)
'''

import os, sys, logging

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
tools_path = os.path.abspath(os.path.join(BASE_DIR, '../tools/'))

sys.path.append(tools_path)
import prosper_api_tools
import util_funcs

# logging setup
logfile = os.path.abspath(os.path.join(BASE_DIR, '../logs/prosper_owned_loans_ETL.log'))
logging.basicConfig(
    filename=logfile,
    format='%(asctime)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

logging.info('Starting prosper_owned_loans_ETL.py script...')

# get prosper connection tokens
#==============================
logging.info('initiating conn. to prosper...')
access_response = prosper_api_tools.initiate_conn()
tokens = access_response.json()
logging.info('connection obtained')

# column schema specification
#============================
COLUMN_SCHEMA = [
	"age_in_months",
	"amount_borrowed",
	"borrower_rate",
	"days_past_due",
	"group_leader_award",
	"collection_fees_paid",
	"debt_sale_proceeds_received",
	"interest_paid",
	"late_fees_paid",
	"loan_default_reason",
	"loan_default_reason_description",
	"loan_number",
	"loan_status",
	"loan_status_description",
	"next_payment_due_date",
	"next_payment_due_amount",
	"origination_date",
	"principal_balance",
	"principal_paid",
	"prosper_fees_paid",
	"prosper_rating",
	"service_fees_paid",
	"term"
]

# get data and write to file
#===========================
file_dir = os.path.join(BASE_DIR, '../data/myloans/')
file_path = 'myloans.bz2'
full_path = os.path.join(file_dir, file_path)

logging.info('initiating data pull...')
response = prosper_api_tools.get_loans_page(tokens, offset=0)  # only returns on success
loans_processed, total_count = util_funcs.write_response_to_disk(
    response, full_path, column_schema=COLUMN_SCHEMA, cur_iter=0
)

# write to disk (following pages)
while loans_processed < total_count:
    # print progress
    progress_rep = f'loans processed: {loans_processed}  total_count: {total_count}'
    print(progress_rep, end='\r')
    # get next page
    response = prosper_api_tools.get_loans_page(tokens, loans_processed)
    # write to disk
    res_count, tcnt = util_funcs.write_response_to_disk(
        response, full_path, column_schema=COLUMN_SCHEMA, mode='a', cur_iter=loans_processed, total_count=total_count
    )
    # update iters
    total_count = tcnt  # (note: this could increase if notes are purchased during query)
    loans_processed += res_count
    print('\b'*len(progress_rep), end='\r')
progress_rep = f'loans processed: {loans_processed}  total_count: {total_count}'
print(progress_rep)
logging.info('done')
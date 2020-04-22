# -*- coding: utf-8 -*-

import os, sys, logging
from pandas import read_csv, Series

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
tools_path = os.path.abspath(os.path.join(BASE_DIR, '../tools/'))

sys.path.append(tools_path)
import prosper_api_tools

# logging setup
LOGGING_LEVEL = logging.INFO
logfile = os.path.abspath(os.path.join(BASE_DIR, '../logs/prosper_payments_ETL.log'))
logging.basicConfig(
    filename=logfile,
    format='%(asctime)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=LOGGING_LEVEL
)

logging.info('Starting prosper_payments_ETL.py script...')

# get prosper connection tokens
#==============================
logging.info('initiating conn. to prosper...')
access_response = prosper_api_tools.initiate_conn()
tokens = access_response.json()
logging.info('connection obtained')

# column schema
#==============
COLUMN_SCHEMA = [
	"loan_number",
	"transaction_id",
	"funds_available_date",
	"investor_disbursement_date",
	"transaction_effective_date",
	"account_effective_date",
	"payment_transaction_code",
	"payment_status",
	"match_back_id",
	"prior_match_back_id",
	"loan_payment_cashflow_type",
	"payment_amount",
	"principal_amount",
	"interest_amount",
	"origination_interest_amount",
	"late_fee_amount",
	"service_fee_amount",
	"collection_fee_amount",
	"gl_reward_amount",
	"nsf_fee_amount",
	"pre_days_past_due",
	"post_days_past_due",
	"resulting_principal_balance"
]

# get loan ids
#=============
LOAN_DIR = os.path.abspath(os.path.join(BASE_DIR, '../data/myloans'))
myloans_file = 'myloans.bz2'
BATCH_SIZE = 25
if myloans_file in os.listdir(LOAN_DIR):
    # get loan numbers from file
    try:
        logging.info(f'retrieving loan numbers from {myloans_file} in {LOAN_DIR}')
        loan_nums = read_csv(
            os.path.join(LOAN_DIR, myloans_file), usecols=['loan_number', 'origination_date'], compression='bz2'
        )
        loan_nums = loan_nums.sort_values(by='origination_date')  # this reduces the amount of payment queries
        loan_nums = loan_nums['loan_number'].values.flatten().astype(str)
    except Exception as e:
        logging.exception(f'issue retrieving loan numbers: {str(e)}')
        logging.debug(f'directory: {LOAN_DIR}  file: {myloans_file}')
        sys.exit(1)
else:
    logging.error(f'could not find {myloans_file} in {LOAN_DIR}')
    sys.exit(1)
    # (note: perhaps we should initiate loan number retrieval from  Prosper in this case)

batches = [loan_nums[i:min(i+BATCH_SIZE, len(loan_nums))] for i in range(0, len(loan_nums), BATCH_SIZE)]
# unit test for no duplicate loan numbers
decomposed_batches = []
[decomposed_batches.extend(b) for b in batches]
print(Series(decomposed_batches).duplicated().any())
assert Series(decomposed_batches).duplicated().any() == False

# get payment data and write to file
#===================================
file_dir = os.path.join(BASE_DIR, '../data/myloans/')
file_path = 'myloan_payments.bz2'
full_path = os.path.join(file_dir, file_path)
logging.info(f'backing up old file: {full_path} to {full_path+".bak"}')
if os.path.exists(full_path):  # backup old file first
    os.rename(full_path, full_path+'.bak')
logging.info(f'backed up old file')

logging.info('initiating payment data pull...')
for i, loan_batch in enumerate(batches):
#    fmode = 'w' if i==0 else 'a'
#    logging.debug(f'fmode set to {fmode} on iteration {i}')
    try:
        logging.debug(f"initiating query for loans: {','.join(loan_batch)}")
        prosper_api_tools.get_many_payments(
            full_path, tokens, column_schema=COLUMN_SCHEMA, loan_number=','.join(loan_batch)#, file_mode=fmode
        )
    except Exception as e:
        logging.exception(f'couldn\'t pull data: {str(e)}')
        sys.exit(1)
    progress_stmt = f'{(i+1)*BATCH_SIZE} loans processed of {len(loan_nums)}'
    if i*BATCH_SIZE % 200 == 0:
        print()
logging.info('done')
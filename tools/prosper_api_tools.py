"""
The request functions enable the password-flow described on the prosper website:
https://developers.prosper.com/docs/authenticating-with-oauth-2-0/password-flow/
"""

import requests, logging, time, sys, os
import util_funcs
import datetime as dt
from pandas import date_range


# setup session access
#=====================
def request_access(client_id, client_secret, username, password):
    '''Use this function on first access. A dictionary is returned with the following format:
        {
           "access_token": "22a5aaaf-bb7b-4278",
           "token_type": "bearer",
           "refresh_token": "7fcb8a8a-e7dd-4fa9",
           "expires_in": 3599
        }
    The refresh_token expires in 10 hours. This may be used to get a new token with the 
    request_refresh method. While active, the access_token may be used to make calls to Prosper APIs.'''
    url = "https://api.prosper.com/v1/security/oauth/token"
    payload = ("grant_type=password&client_id=%s&client_secret=%s" %(client_id, client_secret) +
               "&username=%s&password=%s" %(username, password))
    headers = { 'accept': "application/json",
                'content-type': "application/x-www-form-urlencoded" }
    response = requests.request("POST", url, data=payload, headers=headers)
    return response

def request_refresh(client_id, client_secret, token):
    '''Use this function to get a new access_token within the refresh token window.'''
    url = "https://api.prosper.com/v1/security/oauth/token"
    payload = ("grant_type=refresh_token&client_id=%s&client_secret=%s" %(client_id, client_secret) +
               "&refresh_token=%s" %(token))
    headers = { 'accept': "application/json",
                'content-type': "application/x-www-form-urlencoded" }
    response = requests.request("POST", url, data=payload, headers=headers)
    return response

def initiate_conn():
    import creds
    pc = creds.ProsperClient()
    for i in range(3):
        access_response = request_access(pc.id, pc.secret, pc.username, pc.password)
        if access_response.status_code != 200:
            logging.error(f'try #{i+1}: trouble with connection to Prosper: code {access_response.status_code}')
            time.sleep(2)
            if i == 2:
                sys.exit(1)
        else:
            logging.info('established connection to prosper')
        break
    del pc
    return access_response

# General API
#============
def get_request(url, token_json, timezn='America/Denver', tries=3):
    headers={
        'Authorization': f'bearer {token_json["access_token"]}',
        'Accept': 'application/json',
        'timezone': timezn
    }
    for i in range(tries):
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:  # success!
            break
        elif ((response.status_code == 403) & (response.json() == {"code":"SEC0002","message":"Invalid token"})):
            logging.info('token expired; attempting refresh ...')
            # refresh token
            import creds
            pc = creds.ProsperClient()
            refresh_tkn = token_json['refresh_token']
            token_json = request_refresh(pc.id, pc.secret, refresh_tkn)
            del pc
            logging.info('token refreshed')
        elif ((response.status_code == 401)):
            logging.error(f'bad Prosper credentials: {response.text}')
            sys.exit(1)
        else:
            logging.warning(
                f'try #{i+1}: trouble getting data: code {response.status_code}: response: {response.text}'
            )
            if i+1 == tries:  # fail at 3 tries
                logging.error('script failed after too many tries')
                sys.exit(1)
    return response


# Loans API
#==========
def get_loans_page(token_json, offset, limit=25, sort_by='origination_date', timezn='America/Denver'):
    '''Get loan data from Prosper on loans that are owned. See API documentation for full details,
    query parameters, and examples:
        https://developers.prosper.com/docs/investor/loans-api/
    Note that the offset default is 0, and the limit default/max is 25.
    '''
    url = f'https://api.prosper.com/v1/loans/?offset={offset}&limit={limit}&sort_by={sort_by}'
    response = get_request(url, token_json, timezn)
    return response  # only returns on success

def get_many_loans():
    pass  # TODO


# Notes API
#==========
def get_notes_page(token_json, offset, limit=25, sort_by='origination_date', timezn='America/Denver'):
    '''Get a page of notes from Prosper.'''
    url = f'https://api.prosper.com/v1/notes/?offset={offset}&limit={limit}&sort_by={sort_by}'
    response = get_request(url, token_json, timezn)
    return response  # only returns on success

def get_many_notes(fpath, token_json, limit=50, column_schema=None, timezn='America/Denver'):
    '''Get all of my notes from Prosper.'''
    # get first page
    offset = 0
    limit = 50
    logging.debug('getting first page...')
    response = get_notes_page(token_json, offset, limit)
    logging.debug('writing first page to disk...')
    notes_processed, total_notes = util_funcs.write_response_to_disk(response, fpath, column_schema, 'w')
    logging.debug('finished first page write')
    #get subsequent pages
    logging.debug('getting subsequent pages...')
    while notes_processed < total_notes:
        progress_rep = f'notes processed: {notes_processed}  total_notes: {total_notes}'
        print(progress_rep, end='')
        response = get_notes_page(token_json, notes_processed, limit)
        notes_proc, tnts = util_funcs.write_response_to_disk(
            response, fpath, column_schema, 'a', cur_iter=notes_processed, total_count=total_notes
        )
        # update iters
        notes_processed += notes_proc
        total_notes = tnts
        print('\b'*len(progress_rep), end='')
    progress_rep = f'notes processed: {notes_processed}  total_notes: {total_notes}'
    print(progress_rep)
    return 1


# Listings API
#=============
def get_listings_page(
        token_json, offset, limit=100,
        include_credit_bureau_values='experian,transunion',
        biddable='false',
        invested='true',
        sort_by='listing_start_date',
        timezn='America/Denver'
    ):
    '''Get a page from Prosper listings.
    See API details at: https://developers.prosper.com/docs/investor/listings-api/
    
    Note: All listings objects that were generated prior to March 31st, 2017 contained Experian credit bureau data. After March 31st, 2017, all new listings contain only TransUnion credit bureau data.'''
    
    BASE_ADDRESS = 'https://api.prosper.com/listingsvc/v2/listings/'  # (Different for listings)
    q1 = f'?offset={offset}&limit={limit}&include_credit_bureau_values={include_credit_bureau_values}'
    if invested not in ('null','Null','NULL', None):
        q2 = f'&biddable={biddable}&invested={invested}&sort_by={sort_by}'
    else:
        q2 = f'&biddable={biddable}&sort_by={sort_by}'
    url = BASE_ADDRESS + q1 + q2
    response = get_request(url, token_json, timezn)
    return response

def get_many_listings(fpath, token_json, biddable, invested, column_schema=None, sort_by='listing_start_date'):
    # get first page
    response = get_listings_page(token_json, 0, biddable=biddable, invested=invested)
    listings_processed, total_count = util_funcs.write_response_to_disk(response, fpath, column_schema)
    while listings_processed < total_count:
        # print progress
        progress_rep = f'listings processed: {listings_processed}  total_count: {total_count}'
        print(progress_rep, end='\r')
        # get next page, write to file
        response = get_listings_page(token_json, listings_processed, biddable=biddable, invested=invested)
        li_proc, tcnt = util_funcs.write_response_to_disk(
            response, fpath, column_schema, mode='a', cur_iter=listings_processed, total_count=total_count
        )
        # update iters
        listings_processed += li_proc
        total_count = tcnt  # (count may change as query runs; although this is probably rare)
        print('\b'*len(progress_rep), end='\r')
    progress_rep = f'listings processed: {listings_processed}  total_count: {total_count}'
    print(progress_rep, end='\n')
    return 1
        
    
def get_all_owned_listings(fpath, token_json, column_schema=None, sort_by='listing_start_date'):
    res = get_many_listings(
        fpath, token_json, biddable='false', invested='true', column_schema=column_schema, sort_by='listing_start_date'
    )
    return res
    
def get_active_listings(fpath, token_json, sort_by='listing_start_date'):
    res = get_many_listings(fpath, token_json, biddable='true', invested='null', sort_by='listing_start_date')
    return res

def get_unbid_active_listings(fpath, token_json, sort_by='listing_start_date'):
    res = get_many_listings(fpath, token_json, biddable='true', invested='false', sort_by='listing_start_date')
    return res

def get_bid_on_active_listings(fpath, token_json, sort_by='listing_start_date'):
    res = get_many_listings(fpath, token_json, biddable='true', invested='true', sort_by='listing_start_date')
    return res

def update_owned_listings():
    pass


# Get payments
#=============
def get_payment_page(
        token_json, offset, loan_number, limit=100,
        transaction_effective_date=None,
        timezn='America/Denver'
    ):
    '''Get a page from Prosper listings.
    See API details at: https://developers.prosper.com/docs/investor/payments-api/
    '''
    BASE_ADDRESS = 'https://api.prosper.com/v1/loans/payments/'
    q1 = f'?offset={offset}&limit={limit}'
    q2 = f'&transaction_effective_date={transaction_effective_date}' if transaction_effective_date else ''
    q3 = f'&loan_number={loan_number}' if loan_number else ''
    url = BASE_ADDRESS + q1 + q2 + q3
    response = get_request(url, token_json, timezn)
    return response

def get_many_payments(
        fpath, token_json, loan_number, limit=100,
        # file_mode='w',
        column_schema=None, transaction_effective_date=None,
        timezn='America/Denver'
        ):
    '''note: loan_number can be a list of loans separated by commas.
    transaction_effective_date format is 'yyyy-mm-dd'
    '''
    print(f'Retrieving payments for loans: {loan_number}')
    if transaction_effective_date:
        # get transactions for each 90 day period
        te_date = dt.datetime.strptime(transaction_effective_date, '%Y-%m-%d').date()
        date_periods = date_range(te_date, dt.datetime.now().date(), freq='90D')
    else:
        # get first loan date
        try:
            first_owned_loan = get_loans_page(token_json, 0, limit=1)
            first_date = first_owned_loan.json()['result'][0]['origination_date']
        except Exception as e:
            logging.exception(f'couldn\'t get first loan date: {str(e)}')
        date_periods = date_range(first_date, dt.datetime.now().date(), freq='90D')
    for i, d in enumerate(date_periods):
        # get first page
        stmt = f'Retrieving payments for 90 day period starting: {d.strftime("%Y-%m-%d")}    '
        logging.debug(stmt)
        response = get_payment_page(token_json, 0, loan_number=loan_number, transaction_effective_date=d.strftime('%Y-%m-%d'))
#        if (file_mode == 'w') and (i==0):  # on very first iteration check file_mode; overwrite previous file if 'w'
        if not os.path.exists(fpath):
            logging.debug('overwriting old payments file')
            payments_processed, total_count = util_funcs.write_response_to_disk(
                response, fpath, mode='w', column_schema=column_schema
            )
#        elif (file_mode == 'a') | ((file_mode == 'w') and (i > 0)):  # append on subsequent iterations, or if file_mode is 'a''
        else:
            payments_processed, total_count = util_funcs.write_response_to_disk(
                response, fpath, mode='a', column_schema=column_schema
            )
#        else:
#            logging.exception(f'incorrect file mode value (\'{file_mode}\') passed.')
#            sys.exit(1)
        # get subsequent pages
        while payments_processed < total_count:
            progress_rep = f'payments processed: {payments_processed}  total_count: {total_count}'
            print(progress_rep, end='')
            response = get_payment_page(
                token_json, payments_processed, loan_number=loan_number, transaction_effective_date=d.strftime('%Y-%m-%d')
            )
            pay_proc, tcnt = util_funcs.write_response_to_disk(
                response, fpath, mode='a', column_schema=column_schema,
                cur_iter=payments_processed, total_count=total_count
            )
            # update iters
            payments_processed += pay_proc
            total_count = tcnt
            print('\b'*len(progress_rep), end='')  # reset progress bar
#        if i == (len(date_periods) - 1):  # at very end of for loop
#            progress_rep = f'payments processed: {payments_processed}  total_count: {total_count}'
#            print(progress_rep)
    return 1

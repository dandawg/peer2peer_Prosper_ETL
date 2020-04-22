
import logging, sys
from pandas import DataFrame

# write data to disk
def write_response_to_disk(response, file_path, column_schema=None, mode='w', cur_iter='na', total_count='na'):
    '''Write data to disk. 
    response is an expected return object from `requests.request`.
    `file_path` is the full path to where the file will be written (appended).
    Returns a tuple of the result count and the total count of objects from the response.
    '''
    try:
        tcnt = response.json()['total_count']
        res_cnt = response.json()['result_count']
        if column_schema:
            df = DataFrame(response.json()['result'], columns=column_schema)
            if set(df.columns) != set(column_schema):
                logging.warn('response columns don\'t match schema')
                logging.warn(f'response columns: {df.columns}')
                logging.warn(f'schema columns: {column_schema}')
                # logging.exception('')
                # sys.exit(1)
        else:
            logging.warn('no column schema provided: columns may conflict accross data pulls')
            df = DataFrame(response.json()['result'])
        if (mode == 'w') and (int(res_cnt)>0):
            logging.debug('writing new csv file')
            df.to_csv(file_path, index=False, compression='bz2', mode='w')
        elif (mode == 'a') and (int(res_cnt)>0):
            logging.debug('appending to existing csv')
            df.to_csv(file_path, index=False, compression='bz2', mode='a', header=False)
        elif int(res_cnt)>0:
            raise Exception('mode must be set to "w" or "a"')
        else:
            logging.debug(f'file mode was {mode}')
            logging.debug(f'results = {response.json()}')
            pass  # no data to write
    except Exception as e:
        logging.exception(
            f'on write at iteration {cur_iter} of {total_count}: {str(e)}'
        )
    return (int(res_cnt), int(tcnt))
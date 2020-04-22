# Prosper P2P Lending Data ETL Through Prosper's API

This repo contains scripts to extract loan, note, listing, and payments data from the Prosper peer-to-peer lending platform developer's API.

I developed them as a first step in analyzing my own personal holdings in Prosper, and I'm sharing them as they may be useful to others.

To extract personal data, the `tools/creds.py` file will need to be updated with credentials for a Prosper account (learn more about how to set up credentials and use Prosper's API at https://developers.prosper.com/support/). I had the `tools/creds.py` extract credentials from the local keyring, which is a better option than storing them explicitely in the file.

Once you have credentials, the data extraction scripts in the `prosper_data_ETL` directory can be run as follows:

```python prosper_owned_listing_data_ETL.py```

There are four data extraction scripts:
1. `prosper_owned_listing_data_ETL.py`
2. `prosper_payments_ETL.py`
3. `prosper_notes_ETL.py`
4. `prosper_owned_loans_ETL.py`

These scripts expect `data` and `logs` directories at the top of the repo (for example `peer2peer_Prosper_ETL/data`). Please create these paths and make sure they are available before running extraction.

Also, be advised that Prosper's API only allows for a certain number of records to be extracted at a time, so the scripts may take some time to run if you own a lot of loans. However, once the data is downloaded it is full of good information on loan and listing records.

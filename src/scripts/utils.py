import snowflake.connector
import argparse
import os


BLAST_API_URL = 'https://starknet-mainnet.public.blastapi.io/'


def get_connection():
    snowflakeuser, snowflakepass = os.environ['SNOWFLAKE_USER'], os.environ['SNOWFLAKE_PASS']
    account = os.environ['SNOWFLAKE_ACCOUNT']
    cnx = snowflake.connector.connect(
        user=snowflakeuser,
        password=snowflakepass,
        account=account,
        warehouse='COMPUTE_WH',
        role='ACCOUNTADMIN',
        database='DEV_FEES',
        schema ='PUBLIC'
        )
    return cnx


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('start_block', type=int)
    parser.add_argument('end_block', type=int)
    return parser


    

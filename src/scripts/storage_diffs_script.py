import asyncio
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from starknet_py.net.full_node_client import FullNodeClient, ClientError
from starknet_py.utils.typed_data import get_hex
from utils import get_connection, parser, BLAST_API_URL


INCREMENT = 100 # Size of block batches which are processed in the main loop.


async def main():
    args = parser().parse_args()
    start_block, end_block = args.start_block, args.end_block
    cnx = get_connection()

    full_node_client = FullNodeClient(node_url=BLAST_API_URL)
    failed_blocks = []
    for start in range(start_block, end_block + 1, INCREMENT):
        # Empty dataframe.
        df = pd.DataFrame(columns = ['BLOCK_NUMBER', 'CONTRACT', 'UPDATES_PER_BLOCK'])
        end = min(end_block + 1, start + INCREMENT)
        for block in range(start, end):
            try:
                call_result = await full_node_client.get_state_update(block_number=block)
                diffs = call_result.state_diff.storage_diffs
                for item in diffs:
                    # Check that the storage diffs entries are non-empty;
                    # (some are empty due to a contract nonce update without storage updates).
                    if len(item.storage_entries):
                        new_entry = {
                            'BLOCK_NUMBER': block, 
                            'CONTRACT': get_hex(item.address), 
                            'UPDATES_PER_BLOCK': len(item.storage_entries)
                        }
                        df = pd.concat([df, pd.DataFrame([new_entry])], ignore_index=True)
            # Handle timeout failure and client error.
            except (asyncio.TimeoutError, ClientError):
                failed_blocks.append(f'{block}')
                continue
        success, _, _, _ = write_pandas(cnx, df, 'STORAGE_DIFFS_SCRIPT', auto_create_table=True)
        # Handle failure due to writing to snowflake server.
        if not success:
            failed_blocks.append(f'[{start}:{end}]')
    if failed_blocks:
        with open(f'./storage_script_failed_blocks_{start_block}_{end_block}', 'w') as f:
            f.write(','.join(failed_blocks))


if __name__ == '__main__':
    asyncio.run(main())

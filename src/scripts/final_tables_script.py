import pandas as pd
import queries.generators
from utils import get_connection, parser


DEFAULT_PER_STEP = 0.01 # Gas per Cairo step.
DEFAULT_PER_DIFF = 1024 # Gas per key-value: 16 gas per byte, i.e. 512 gas per word (32B), multiplied by 2.
NAMED_CONTRACTS_CSV_PATH = '../csv/names.csv' # Contains addresses with their tag.
CSV_FOLDER_PATH = '../csv'


def create_table(df, version):
    names = pd.read_csv(NAMED_CONTRACTS_CSV_PATH)
    tag_list = []
    for contract in df['CONTRACT']:
        res = names[names['CONTRACT'] == contract]['NAMES']
        tag_list.append(res.values[0] if res.shape[0] else 0)
    df['NAMES'] = pd.Series(tag_list)
    df.to_csv(f"{CSV_FOLDER_PATH}/fee_amounts_v{version}.csv", float_format='{:.20f}'.format)


if __name__ == '__main__':
    parser = parser()
    parser.add_argument('version', type=str)
    args = parser.parse_args()
    start_block, end_block, version = args.start_block, args.end_block, args.version
    cs = get_connection().cursor()

    # Pipeline to build all the tables except the 'cairo_steps_script' and 'storage_diffs_script' tables.
    pipe_queries = [
        queries.generators.block_fee(start_block=start_block, end_block=end_block),
        queries.generators.builtin_gas(start_block=start_block, end_block=end_block),
        queries.generators.diffs_per_contract_per_block(),
        queries.generators.steps_per_contract_per_block(),
        queries.generators.join_steps_and_diffs(),
        queries.generators.final(),
        queries.generators.final_proportions(gas_per_step=DEFAULT_PER_STEP, gas_per_diff=DEFAULT_PER_DIFF),
        queries.generators.final_fee_divided(),
        queries.generators.ranking_l1_l2()
    ]

    for query in pipe_queries:
        cs.execute(query)
    df = cs.fetch_pandas_all()
    create_table(df, version)
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import queries.generators
from utils import get_connection, parser


INCREMENT = 100 # Size of block batches which are processed in the main loop.


class Tree:
    """
    Class that represents the transaction tree, for a set of transactions. 
    Each internal call is represented as a tuple of elements 
    in {str(int), character 'v', character 'f'}, corresponding to its trace_id.
    Examples of such tuples: ('10', '1'), ('1', 'v').
    The trace_id is a string of the form '{block_number}_{tx_index}_{int}_{int}_{int} ... ', 
    and it uniquely identifies an internal call in the history of the network.
    The class Tree stores two dicts:
        - dict_steps: node -> number steps
        - dict_children: node -> list of children nodes
    The dict_steps dictionary will be initialized with the data from the tokenflow database.
    """
    # The constructor receives an *ordered* list of pairs (tuple(str),int), where the order is lexicographic
    # by the first element of the pair.
    def __init__(self, trace_id_and_steps: list((tuple[str],int))):
        self.dict_steps = {tuple(): 0} # The empty string is the common ancestor.
        self.dict_children = {tuple(): []}
        for node, steps in trace_id_and_steps:
            node = tuple(node)
            parent_node = node[:-1]
            self.dict_children[node] = []
            # The letters 'v' and 'f' correspond to the validation internal call and the fee payment internal call.
            # Here we use that the trace_id_and_step is ordered lexicographically:
            # if the parent_node is valid and shorter than node, then it's already in the dictionary.
            if parent_node not in self.dict_children or node[-1] in ('v','f'):
                self.dict_children[tuple()].append(node)
            else:
                self.dict_children[parent_node].append(node)
            self.dict_steps[node] = steps

    def infer_steps(self, node: tuple[str]) -> int:
        """
        Given a tree node 'node', the function changes the entry of node in dict_steps[node]
        to match the actual steps done by that internal call, and returns the original value
        of dict_steps[node] (i.e. the sum of the number of steps done by that node and its children)
        """
        ret = self.dict_steps[node] # This is the returned value of the method.
        # If a node is a leaf, the steps of the internal call are already stored in the dictionary.
        if len(self.dict_children[node]) == 0:
            return ret
        # Compute the total steps made by the children nodes of 'node', and then subtract it from the
        # number of steps which stored in dict_steps[node].
        steps_of_children = 0
        for child in self.dict_children[node]:
            steps_of_children += self.infer_steps(child)
        self.dict_steps[node] -= steps_of_children # Change the record in the dict_steps dictionary.
        return ret

    def infer_all(self) -> None:
        """
        Infer steps for all nodes in the tree. Must be called only once.
        """
        # The empty list is an ancestor of all the nodes in the tree
        # and is a parent of the first call (or calls, if tx is a multicall).
        self.infer_steps(tuple())

    def to_data_frame(self) -> pd.DataFrame:
        """
        Transforms a Tree into a dataframe with an 'INDIVIDUAL_STEPS' column.
        """
        # The elements of _result are pairs of the form (tuple[str], int).
        _result = list(self.dict_steps.items())
        result = [x for x in _result if len(x[0]) != 0]
        df = pd.DataFrame.from_records(data=result, columns=['SPLIT_TRACE_ID', 'INDIVIDUAL_STEPS'])
        df = df.sort_values(by='SPLIT_TRACE_ID')
        return df


def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the output of an SQL query into a dataframe that
    can be passed to the Tree class. In particular, it orders the dataframe
    by the 'TRACE_ID' column.
    """
    df['STEPS'] = df['STEPS'].fillna(0).astype(int)
    df['SPLIT_TRACE_ID'] = df['TRACE_ID'].str.split("_").map(lambda x : tuple(x))
    df.sort_values(by='SPLIT_TRACE_ID', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def main():
    args = parser().parse_args()
    start_block, end_block = args.start_block, args.end_block
    cnx = get_connection()
    cs = cnx.cursor()
    failed_blocks = []
    for start in range(start_block, end_block + 1, INCREMENT):
        end = min(end_block, start + INCREMENT - 1)
        cs.execute(queries.generators.traces(start, end))
        if not cs:
            failed_blocks.append(f'[{start}-{end}]')
            continue
        df = format_dataframe(cs.fetch_pandas_all())

        tree = Tree(list(zip(df['SPLIT_TRACE_ID'], df['STEPS'])))
        tree.infer_all()
        output_df = tree.to_data_frame()
        df['INDIVIDUAL_STEPS'] = output_df['INDIVIDUAL_STEPS']
        df['SPLIT_TRACE_ID_TEST'] = output_df['SPLIT_TRACE_ID']
        # Check that the trace is consistent after the running the Tree methods.
        assert all(df['SPLIT_TRACE_ID'] == df['SPLIT_TRACE_ID_TEST'])
        # Drop the testing columns.
        df.drop(columns=['SPLIT_TRACE_ID', 'SPLIT_TRACE_ID_TEST'], inplace=True)
        success, _, _, _ = write_pandas(cnx, df, 'CAIRO_STEPS_SCRIPT', auto_create_table=True)
        if not success:
            failed_blocks.append(f'[{start}:{end}]')
    if failed_blocks:
        with open(f'./cairo_script_failed_blocks_{start_block}_{end_block}', 'w') as f:
            f.write(','.join(failed_blocks))


if __name__ == '__main__':
    main()

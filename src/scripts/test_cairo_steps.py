import pandas as pd
import unittest
from cairo_steps_script import Tree
from utils import get_connection


MAX_BLOCK = 448500
MIN_BLOCK = 1


class TreeTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        tree_1 = [(('10', '7'), 100),
            (('10', '7', '0'), 10),
            (('10', '7', '1'), 11),
            (('10', '7', '2'), 12),
            (('10', '7', '2', '0'), 10),
            (('10', 'f'), 20),
            (('10', 'f', 0), 5)
        ]

        tree_1_true = [(('10', '7'), 67),
            (('10', '7', '0'), 10),
            (('10', '7', '1'), 11),
            (('10', '7', '2'), 2),
            (('10', '7', '2', '0'), 10),
            (('10', 'f'), 15),
            (('10', 'f', 0), 5)
        ]

        tree_2 = [(('10', '7'), 6662),
            (('10', '7', '0'), 177),
            (('10', '7', '1'), 4430),
            (('10', '7', '1', '0'), 4370),
            (('10', '7', '1', '0', '0'), 94),
            (('10', '7', '1', '0', '1'), 866),
            (('10', '7', '1', '0', '2'), 590),
            (('10', '7', '1', '0', '2', '0'), 530),
            (('10', '7', 'v'), 1619),
            (('10', '7', 'f'), 590),
            (('10', '7', 'f', '0'), 530)
        ]

        tree_2_true = [(('10', '7'), 2055),
            (('10', '7', '0'), 177),
            (('10', '7', '1'), 60),
            (('10', '7', '1', '0'), 2820),
            (('10', '7', '1', '0', '0'), 94),
            (('10', '7', '1', '0', '1'), 866),
            (('10', '7', '1', '0', '2'), 60),
            (('10', '7', '1', '0', '2', '0'), 530),
            (('10', '7', 'v'), 1619),
            (('10', '7', 'f'), 60),
            (('10', '7', 'f', '0'), 530)
        ]
        
        self.trees = [(tree_1, tree_1_true), (tree_2, tree_2_true)]
        return None

    def test_individual_steps(self):
        for tree, tree_true in self.trees:
            curr_tree = Tree(tree)
            curr_tree.infer_all()
            set_tree = set([x for x in curr_tree.dict_steps.items() if len(x[0])!= 0])
            set_tree_true = set(tree_true)
            self.assertSetEqual(set_tree, set_tree_true)


class TableScriptTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.cs = get_connection().cursor()

    def test_max_block_number(self):
        query = """
        select
            max(block_number) as "max"
        from cairo_steps_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'max'], MAX_BLOCK)
        return None
    
    def test_min_block_number(self):
        query = """
        select
            min(block_number) as "min"
        from cairo_steps_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'min'], MIN_BLOCK)
    
    def test_number_of_blocks(self):
        query = """
        select
            count(distinct block_number) as "count"
        from cairo_steps_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'count'], MAX_BLOCK - MIN_BLOCK + 1)
    
    def test_duplicates(self):
        query = """
            select
                cairo_steps_script.trace_id,
                count(*)
            from cairo_steps_script
            group by cairo_steps_script.trace_id
            having count(*) > 1
            ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.shape[0], 0)

    def test_total_steps(self):
        starkscan_steps = {
            10: 1697,
            100: 6569,
            1000: 30246,
            10000: 363137,
            100000: 1804945
        }
        for block in starkscan_steps.keys():
            query = f"""
                select
                    cairo_steps_script.individual_steps,
                    cairo_steps_script.trace_id
                from cairo_steps_script
                where block_number = {block}
                ;
            """
            df = self.cs.execute(query).fetch_pandas_all()
            # Select all internal calls different from validate and from fee transfer.
            # The sum of steps of the remaining calls should be equal to the number
            # of steps found on block explorers.
            df = df[(~df['TRACE_ID'].str.contains('v')) & (~df['TRACE_ID'].str.contains('f'))]
            self.assertEqual(df['INDIVIDUAL_STEPS'].sum(), starkscan_steps[block])
        

if __name__ == '__main__':
    unittest.main()
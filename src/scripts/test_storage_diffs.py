import pandas as pd
from utils import get_connection
import unittest


MAX_BLOCK = 448500
MIN_BLOCK = 1


class TableScriptTests(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.cs = get_connection().cursor()

    def test_max_block_number(self):
        query = """
        select
            max(block_number) as "max"
        from storage_diffs_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'max'], MAX_BLOCK)
        return None
    
    def test_min_block_number(self):
        query = """
        select
            min(block_number) as "min"
        from storage_diffs_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'min'], MIN_BLOCK)
    
    def test_number_of_blocks(self):
        query = """
        select
            count(distinct block_number) as "count"
        from storage_diffs_script
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'count'], MAX_BLOCK - MIN_BLOCK + 1)
    
    def test_duplicates(self):
        query = """
            select
                block_number,
                contract,
                updates_per_block,
                count(*)
            from storage_diffs_script
            group by block_number, contract, updates_per_block
            having count(*) > 1
            ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.shape[0], 0)


if __name__ == '__main__':
    unittest.main()
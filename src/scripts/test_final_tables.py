import pandas as pd
import unittest
from utils import get_connection


MAX_BLOCK = 448500
MIN_BLOCK = 1


class FinalTablesTests(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.cs = get_connection().cursor()

    def test_max_block_number(self):
        """
        Test if the biggest block number 
        appearing in final table is correct
        """

        query = """
        select
            max(block_number) as "max"
        from final
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'max'], MAX_BLOCK)
        return None
    
    def test_min_block_number(self):
        query = """
        select
            min(block_number) as "min"
        from final
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'min'], MIN_BLOCK)
    
    def test_number_of_blocks(self):
        query = """
        select
            count(distinct block_number) as "count"
        from final
        ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.at[0 ,'count'], MAX_BLOCK - MIN_BLOCK + 1)
        
    def test_duplicates(self):
        query = """
            select
                block_number,
                contract,
                steps_per_contract,
                steps_per_block,
                diffs_per_contract,
                diffs_per_block,
                contracts_per_block,
                block_fee,
                count(*)
            from final
            group by
                block_number,
                contract,
                steps_per_contract,
                steps_per_block,
                diffs_per_contract,
                diffs_per_block,
                contracts_per_block,
                block_fee
            having count(*) > 1
            ;
        """
        df = self.cs.execute(query).fetch_pandas_all()
        self.assertEqual(df.shape[0], 0)

    def test_total_fees(self):
        query_from_final = """
            select
                sum(l1_fee_per_contract + l2_fee_per_contract) as "sum"
            from final_fee_amounts_divided
            ;
            """
        query_from_tokenflow = """
            select
                sum(block_fee) as "sum"
            from block_fee
            ;
            """
        sum_final = int(self.cs.execute(query_from_final).fetch_pandas_all().at[0, 'sum'])
        sum_tokenflow = int(self.cs.execute(query_from_tokenflow).fetch_pandas_all().at[0, 'sum'])
        # The numbers will not be exactly equal because of rounding errors in the divisions involved.
        # The assert checks that discrepancy is less than 10 million wei (= fractions of $0.01).
        self.assertTrue(abs(sum_final - sum_tokenflow) <= 10**8)


if __name__ == '__main__':
    unittest.main()
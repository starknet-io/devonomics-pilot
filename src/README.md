# Contents

The folder `scripts` contains the Python scripts used in the first batch of devonomics. The folder `csv` contains `names.csv` which is a list of pairs (address, label), an auxiliary file `top_contracts.csv` used to build `names.csv`, and the file `allocations.csv` which contains the final list of around ~10K contracts ordered by their allocation size. The output of the scripts in `scripts` is a csv table in the folder `csv`, containing a list of addresses with their L1 and L2 fee amounts.

For a more human readable description of the computation and the final results, see [Notions page](https://www.notion.so/starkware/Devonomics-Pilot-Program-4ecf9d8ea5864bb29e347be8926932f5).

**Note**: the scripts connect to a snowflake server (requiring account credentials) which hosts the databases we use. The databases that we use can be found at [https://flipsidecrypto.xyz/](https://flipsidecrypto.xyz/) (under the name Tokenflow Starknet).
# Contents

The folder `scripts` contains the Python scripts used in the first batch of devonomics. The folder `csv` contains `names.csv` which is a list of pairs (address, label), and an auxiliary file `top_contracts.csv` used to build `names.csv`. The output of the scripts in `scripts` is a csv table in the folder `csv`, containing a list of addresses with their amount.

For a more human readable description of the computation and the final results, see [Notions page](https://www.notion.so/starkware/Devonomics-Pilot-Program-4ecf9d8ea5864bb29e347be8926932f5).

**Note**: the scripts connect to a snowflake server (requiring account credentials) which hosts the databases we use. The databases that we use can be found at [https://flipsidecrypto.xyz/](https://flipsidecrypto.xyz/) (under the name Tokenflow Starknet).
# Execution of the scripts

The three files `cairo_steps_script.py`, `storage_diffs_script.py`, `final_tables_script.py` contain the scripts used to generate all the tables employed in the redistribution. The tables are generated on a Snowflake server. The server also contains databases of Starknet's transactions, traces and blocks.

## To run all scripts at once

Run 
`./exec_scripts.sh start_block end_block version`
(`version` is a string that will be appended to the name of the csv file that will be created, containing the distribution per address.)

## To run the scripts individually (in the right order)

1. Run `python cairo_steps_script.py start_block end_block`, e.g. 

`python cairo_steps_script.py 1 10`

The current settings are start_block=1 and end_block=448500.

2. Run `python storage_diffs_script.py start_block end_block`, e.g. 

`python storage_diffs_script.py 1 10`

The current settings are start_block=1 and end_block=448500.

3. Run `python final_tables_script.py start_block end_block version`.
The current settings are start_block=1 and end_block=448500. The final_tables_script saves a ranking of the top 10000 contracts in the folder CSVs as a file with name 'fee_amounts_v{version}.csv'

## Starkscan script

The script `starkscan_query.py` fetches name tags for addresses from Starkscan's API and saves them into a csv file `names.csv` in the folder `csv`.

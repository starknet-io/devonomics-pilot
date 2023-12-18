#!/bin/bash

python cairo_steps_script.py $1 $2 & 
python storage_diffs_script.py $1 $2
wait
python final_tables_script.py $1 $2 $3

#!/bin/bash

git checkout master && git pull
python3 src/extract_covid_data_br.py && git add covid-br*.csv* && git commit -m "updating data" && git push
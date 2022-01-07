#!/bin/bash

printf "\n\n\nstart of run at `date +'%Y-%m-%d %H:%M:%S'`\n"
cd ~/../../data/repos/covid-19_germany/


git status
git pull
git submodule update --remote
git status
/data/tools/anaconda3/bin/python restructure_data.py
git status
git add date.txt
git add source_data_rki.tsv
git add data_max_date.tsv
git add data_rki.tsv
git add data_geo_time.tsv
git add data_time.tsv
git add data_geo.tsv
git add data_mobility.tsv
git add data_stringency.tsv
git status
git commit -m "date update: `date +'%Y-%m-%d %H:%M:%S'`";
git status
git push origin main
git status
git lfs prune

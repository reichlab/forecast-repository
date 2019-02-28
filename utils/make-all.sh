#!/usr/bin/env bash

echo "#### make_minimal_projects.py"
python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_minimal_projects.py

echo "#### make_thai_moph_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_thai_moph_project.py \
    /Users/cornell/IdeaProjects/moph-forecast-files \
    --make_project \
    --load_data

echo "#### make_cdc_flu_contests_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_cdc_flu_contests_project.py \
    /Users/cornell/IdeaProjects/cdc-flusight-ensemble/model-forecasts/component-models \
    /Users/cornell/IdeaProjects/forecast-repository/truths-2010-through-2018.csv

echo "#### fix_owners_app.py"
python3 /Users/cornell/IdeaProjects/forecast-repository/utils/fix_owners_app.py

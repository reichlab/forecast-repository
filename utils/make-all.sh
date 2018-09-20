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
    /Users/cornell/IdeaProjects/split_kot_models_from_submissions \
    /Users/cornell/IdeaProjects/2017-2018-cdc-flu-contest/inst/submissions \
    /Users/cornell/IdeaProjects/cdc-flusight-ensemble/model-forecasts/component-models

echo "#### fix_owners_app.py"
python3 /Users/cornell/IdeaProjects/forecast-repository/utils/fix_owners_app.py

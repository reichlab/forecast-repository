#!/usr/bin/env bash

echo "#### make_minimal_projects.py"
python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_minimal_projects.py

echo "#### make_2017_2018_flu_contest_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_2017_2018_flu_contest_project.py \
    /Users/cornell/IdeaProjects/2017-2018-cdc-flu-contest/inst/submissions \
    --make_project \
    --load_data

echo "#### make_2016_2017_flu_contest_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_2016_2017_flu_contest_project.py \
    /Users/cornell/IdeaProjects/split_kot_models_from_submissions

echo "#### make_thai_moph_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_thai_moph_project.py \
    /Users/cornell/IdeaProjects/moph-forecast-files \
    --make_project \
    --load_data

echo "#### make_cdc_flusight_ensemble_project.py"
time python3 /Users/cornell/IdeaProjects/forecast-repository/utils/make_cdc_flusight_ensemble_project.py \
    /Users/cornell/IdeaProjects/cdc-flusight-ensemble/model-forecasts/component-models \
    --make_project \
    --load_data

echo "#### fix_owners_app.py"
python3 /Users/cornell/IdeaProjects/forecast-repository/utils/fix_owners_app.py

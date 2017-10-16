# Forecast Repository project
This is a Django project to implement a repository of forecast challenges. See the internal
[Forecast repository notes](https://docs.google.com/document/d/1cKQY0tgSR8QkxvJUEuMR1xBCvzNYBnMhkNYgK3hCOsk) document
for a more detailed description. The internal [reichlab Slack](https://reichlab.slack.com) channel for this is 
[#forecast-repository](https://reichlab.slack.com/messages/C57HNDFN0/). The GitHub location is
https://github.com/reichlab/forecast-repository .


# Requirements - see requirements.txt:
pip install django
pip install click
pip install requests
pip install jsonfield
pip install psycopg2


# Utils
The files under utils/ are currently project-specific ones, currently related to making the CDC flu challenge data
amenable to analysis.


# Running the tests
```bash
cd /Users/cornell/IdeaProjects/forecast-repository/forecast_app/tests
python3 ../../manage.py test
```


# TODO

## code
- rename 'forecast_app'?
- model constraints like null=True
- change __str__()s to be prettier
- change app name from forecast_app to something better?
- Bootstrap: download locally-stored libs? bootstrap.min.css , jquery.min.js , nad bootstrap.min.js


## admin
- Project: ForecastModels inline: while ForecastModelAdminLinkInline does work, each instance's __str__() is still
  displayed, which is redundant with the link text. maybe: https://stackoverflow.com/questions/5086537/how-to-omit-object-name-from-djangos-tabularinline-admin-view


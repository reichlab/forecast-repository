# Forecast Repository project
This is a Django project to implement a repository of forecast challenges. See the internal
[Forecast repository notes](https://docs.google.com/document/d/1cKQY0tgSR8QkxvJUEuMR1xBCvzNYBnMhkNYgK3hCOsk) document
for a more detailed description. The internal [reichlab Slack](https://reichlab.slack.com) channel for this is 
[#forecast-repository](https://reichlab.slack.com/messages/C57HNDFN0/). The GitHub location is
https://github.com/reichlab/forecast-repository .


# Requirements (see Pipfile)
- [Python 3](http://install.python-guide.org)
- [pipenv](https://docs.pipenv.org/)
- for Heroku hosting:
  - [Heroku Toolbelt](https://toolbelt.heroku.com/)
  - [Postgres](https://devcenter.heroku.com/articles/heroku-postgresql#local-setup)


To install required packages:
```bash
$ pipenv --three
$ cd <readme.md's dir>/forecast-repository
$ pipenv install
```

Pipfile was created via:
```bash
$ pipenv install django
$ pipenv install click
$ pipenv install requests
$ pipenv install jsonfield
$ pipenv install psycopg2
$ pipenv install dj-database-url
$ pipenv install gunicorn
$ pipenv install whitenoise
```


# Utils
The files under utils/ are currently project-specific ones, currently related to making the CDC flu challenge data
amenable to analysis.


# Running the tests
```bash
$ cd <readme.md's dir>/forecast-repository
$ pipenv shell
$ cd forecast_app/tests
$ python3 ../../manage.py test --verbosity 2 --settings=forecast_repo.settings.local_sqlite3
```

# Django project layout

This project's settings scheme follows the "split settings.py into separate files in their own 'settings' module"
approach. Since we plan on deploying to Heroku, there is no production.py. Regardless, every app needs to set
the `DJANGO_SETTINGS_MODULE` environment variable accordingly, e.g., one of the following:
```bash
$ export DJANGO_SETTINGS_MODULE="forecast_repo.settings.local_sqlite3"
$ ./manage.py migrate --settings=forecast_repo.settings.local_sqlite3
$ heroku config:set DJANGO_SETTINGS_MODULE=forecast_repo.settings.local_sqlite3
gunicorn -w 4 -b 127.0.0.1:8001 --settings=forecast_repo.settings.local_sqlite3
```


# TODO

## code
- rename 'forecast_app'?
- model constraints like null=True
- change __str__()s to be prettier
- change app name from forecast_app to something better?
- Bootstrap: download locally-stored libs? bootstrap.min.css , jquery.min.js , and bootstrap.min.js


## admin changes to make:
- Admin home: /admin/ :
  - shows Project list: /admin/forecast_app/project/ :
- Project list:
  - show only current user's projects, unless superuser
- Project detail:
  - Add/Edit Project: Project.owner forced to current user, unless superuser
  - template field: if template loaded: shows template *.csv name & Delete button. o/w shows '-' & Add button -> form:
- Project > Upload Template form: File input field, Upload and Cancel buttons -> Project.load_template()
- ForecastModel detail:
  - Forecast inline fields: TimeZero, Forecast, Action. latter two: if data loaded: shows template *.csv name (as a link
    to Forecast detail) & Delete button. o/w shows '-' & Add button -> form:
- ForecastModel > Upload Forecast form: File input field, Upload and Cancel buttons -> ForecastModel.load_forecast()
- Forecast detail:
  - all fields read-only. additionally: some kinds of visualizations or report?

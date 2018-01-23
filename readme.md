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
- [Pillow](https://github.com/python-pillow/Pillow)

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
$ pipenv install djangorestframework
$ pipenv install Pillow
```


# Utils
The files under utils/ are currently project-specific ones. They should probably be moved.


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


# Heroku deployment
The site is currently hosted by Heroku at https://reichlab-forecast-repository.herokuapp.com/ . Follow these steps to
update it:


## login
```bash
$ cd ~/IdeaProjects/forecast-repository
$ pipenv shell
$ heroku login
```


## optional: dump local db then copy to remote
```bash
$ PGPASSWORD=mypassword
$ pg_dump -Fc --no-acl --no-owner -h localhost -U cornell forecast_repo > /tmp/mc-1219-forecast_repo.dump
```

- upload to somewhere publicly accessible, e.g., Amazon S3

```bash
$ heroku pg:backups:restore 'https://s3.us-east-2.amazonaws.com/yourbucket/yourdatabase.dump' DATABASE_URL
$ heroku run python manage.py createsuperuser --settings=forecast_repo.settings.heroku_production
```


## push code
```bash
$ git push heroku master
```


## start web dyno
```bash
$ heroku ps:scale web=1
$ heroku open
```

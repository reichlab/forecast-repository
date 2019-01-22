# Zoltar forecast archive project
This is a Django project to implement a repository of forecast challenges. See the internal
[Forecast repository notes](https://docs.google.com/document/d/1cKQY0tgSR8QkxvJUEuMR1xBCvzNYBnMhkNYgK3hCOsk) document
for a more detailed description. The internal [reichlab Slack](https://reichlab.slack.com) channel for this is 
[#forecast-repository](https://reichlab.slack.com/messages/C57HNDFN0/). The GitHub location is
https://github.com/reichlab/forecast-repository .


# Email-based notification requirements
Zoltar uses [Anymail](https://github.com/anymail/django-anymail) to abstract access to the transactional email server
that's used for notifications. (Currently we only have notifications about file uploads.) Anymail can be used for a
number of [services](https://anymail.readthedocs.io/en/stable/esps/#supported-esps), as configured in settings.
Currently we use [SendinBlue](https://www.sendinblue.com/).

Configuration: The environment variable _SENDINBLUE_API_KEY_ must be set, e.g., for Heroku:

```bash
heroku config:set \
  SENDINBLUE_API_KEY=<YOUR_SENDINBLUE_API_KEY>
```


# AWS S3 configuration
Zoltar uses S3 for temporary storage of uploaded files (forecasts, truth, and templates). You'll need to set three 
S3-related environment variables, either locally or, for Heroku:

```bash
heroku config:set \
  S3_UPLOAD_BUCKET_NAME=<bucket name with below access keys> \
  AWS_ACCESS_KEY_ID=<YOUR_ACCESS_KEY> \
  AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_KEY>
```

These keys must enable read, write, and list operations on a bucket named S3_UPLOAD_BUCKET_NAME in that account. For
development that account was configured thus:

- (IAM) Zoltar app user:
  - no groups
  - Permissions > Permissions policies > Attached directly: 
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::mc.zoltarapp.sandbox",
                "arn:aws:s3:::mc.zoltarapp.sandbox/*"
            ]
        }
    ]
}
```
- (S3) Zoltar upload bucket:
  - Permissions > Access control list: default (root)
  - Permissions > Bucket policy: none (controlled above at the user level) 


# Requirements (see Pipfile)
- [Python 3](http://install.python-guide.org)
- [pipenv](https://docs.pipenv.org/)
- for Heroku hosting:
  - [Heroku Toolbelt](https://toolbelt.heroku.com/)
  - [Postgres](https://devcenter.heroku.com/articles/heroku-postgresql#local-setup)
- [Pillow](https://github.com/python-pillow/Pillow)
- [pymmwr](https://github.com/reichlab/pymmwr)

To install required packages:
```bash
$ pipenv --three
$ cd <readme.md's dir>/forecast-repository
$ pipenv install
```

Pipfile was created via:
```bash
pipenv install django
pipenv install click
pipenv install requests
pipenv install jsonfield
pipenv install psycopg2-binary
pipenv install dj-database-url
pipenv install gunicorn
pipenv install whitenoise
pipenv install djangorestframework
pipenv install Pillow
pipenv install pymmwr
pipenv install pyyaml
pipenv install djangorestframework-csv
pipenv install django-debug-toolbar
pipenv install rq
pipenv install django-rq
pipenv install boto3
pipenv install djangorestframework-jwt
pipenv install more-itertools
pipenv install django-anymail[sendgrid,sendinblue]
```


# Utils
The files under utils/ are currently project-specific ones. They should probably be moved.


# RQ infrastructure
Zoltar uses an asynchronous messaging queue to support executing long-running tasks outside the web dyno, which keeps
the latter responsive and prevents Heroku's 30 second timeouts. We use [RQ](https://python-rq.org/) for this, which
requires a [Redis](https://redis.io/) server along with one or more worker dynos. Here's the setup to run locally:

1. Start Redis:
```$bash
redis-server
```

1. Start an rq worker:
```$bash
cd ~/IdeaProjects/django-redis-play
pipenv shell
export PATH="/Applications/Postgres.app/Contents/Versions/9.6/bin:${PATH}" ; export DJANGO_SETTINGS_MODULE=forecast_repo.settings.local_sqlite3 ; export PYTHONPATH=.
python3 manage.py rqworker
```

1. Optionally start monitor (`rq info` or `rqstats`):
```$bash
cd ~/IdeaProjects/django-redis-play
pipenv shell
rq info --interval 1

# alternatively:
export PATH="/Applications/Postgres.app/Contents/Versions/9.6/bin:${PATH}" ; export DJANGO_SETTINGS_MODULE=forecast_repo.settings.local_sqlite3 ; export PYTHONPATH=.
python3 manage.py rqstats --interval 1
```

1. Start the web app
```$bash
cd ~/IdeaProjects/django-redis-play
pipenv shell
export PATH="/Applications/Postgres.app/Contents/Versions/9.6/bin:${PATH}" ; export DJANGO_SETTINGS_MODULE=forecast_repo.settings.local_sqlite3 ; export PYTHONPATH=.
python3 manage.py runserver --settings=forecast_repo.settings.local_sqlite3
```

1. Execute the functions that insert onto the queue, e.g.,
```$bash
cd ~/IdeaProjects/django-redis-play
pipenv shell
export PATH="/Applications/Postgres.app/Contents/Versions/9.6/bin:${PATH}" ; export DJANGO_SETTINGS_MODULE=forecast_repo.settings.local_sqlite3 ; export PYTHONPATH=.
python3 utils/row_count_util.py update
```

1. Optionally monitor the progress in the web app
- [http://127.0.0.1:8000/zadmin](http://127.0.0.1:8000/zadmin)


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
$ PGPASSWORD=password
$ pg_dump -Fc --no-acl --no-owner -h localhost -U username forecast_repo > /tmp/mc-1219-forecast_repo.dump
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

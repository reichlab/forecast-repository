import os


#
# ---- DEBUG and SECRET_KEY ----
#

# NB: must set the following before importing from base

DEBUG = False

if 'SECRET_KEY' not in os.environ:
    from django.core.exceptions import ImproperlyConfigured


    raise ImproperlyConfigured("The 'SECRET_KEY' environment variable was not set.")

SECRET_KEY = os.environ['SECRET_KEY']

#
# ---- imports ----
#

import dj_database_url

from .base import *


#
# ---- database config ----
#

# Update database configuration with $DATABASE_URL. This default is used when running `$ heroku local` b/c the .env
# file sets DJANGO_SETTINGS_MODULE="forecast_repo.settings.heroku_production"
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}

db_from_env = dj_database_url.config(conn_max_age=500)
DATABASES['default'].update(db_from_env)
DATABASES['default']['TEST'] = {'NAME': DATABASES['default']['NAME']}

#
# ---- Django-RQ config ----
#

redis_url = os.environ.get('REDISCLOUD_URL')

if not redis_url:
    raise RuntimeError('heroku_production.py: REDISCLOUD_URL not configured!')

RQ = {
    'WORKER_CLASS': 'scout_apm.rq.Worker',
}

RQ_QUEUES = {
    HIGH_QUEUE_NAME: {
        'URL': redis_url,
        'DEFAULT_TIMEOUT': 500,
    },
    DEFAULT_QUEUE_NAME: {
        'URL': redis_url,
        'DEFAULT_TIMEOUT': 500,
    },
    LOW_QUEUE_NAME: {
        'URL': redis_url,
        'DEFAULT_TIMEOUT': 500,
    },
}

#
# ---- other config ----
#

# Honor the 'X-Forwarded-Proto' header for request.is_secure()
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

# Allow all host headers
ALLOWED_HOSTS = ['*']

#
# ---- https://docs.scoutapm.com/#django ----
#

INSTALLED_APPS = ['scout_apm.django'] + INSTALLED_APPS  # "should be listed first"

# RQ: see RQ['WORKER_CLASS'] above

# Scout settings: configured via Heroku config vars - https://devcenter.heroku.com/articles/config-vars
# SCOUT_MONITOR = True
# SCOUT_KEY = "[AVAILABLE IN THE SCOUT UI]"
# SCOUT_NAME = "A FRIENDLY NAME FOR YOUR APP"

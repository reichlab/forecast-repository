# NB: must set the following before importing from base

DEBUG = True

SECRET_KEY = '&6kqgmf2fi3==##07k$!ns_#sd1%v4e4%$lbgft9(c7ar9itbh'

from .base import *


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'forecast_repo',
        'USER': 'cornell',
        'PASSWORD': '',
        'HOST': '127.0.0.1',
        'PORT': '5432',
    }
}


#
# ---- Django-RQ config ----
#

RQ_QUEUES = {
    HIGH_QUEUE_NAME: {
        'URL': 'redis://localhost:6379/0',
        'DEFAULT_TIMEOUT': 360,
    },
    DEFAULT_QUEUE_NAME: {
        'URL': 'redis://localhost:6379/0',
        'DEFAULT_TIMEOUT': 360,
    },
    LOW_QUEUE_NAME: {
        'URL': 'redis://localhost:6379/0',
        'DEFAULT_TIMEOUT': 360,
    },
}


#
# ---- djangorestframework-jwt config ----
#

# JWT_AUTH = {
#     'JWT_VERIFY_EXPIRATION': False,  # dangerous
# }


#
# http://whitenoise.evans.io/en/stable/django.html#using-whitenoise-in-development
#

INSTALLED_APPS = ['whitenoise.runserver_nostatic'] + INSTALLED_APPS  # put at top, before 'django.contrib.staticfiles'

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
    'default': {
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

# NB: must set the following before importing from base

DEBUG = True

SECRET_KEY = 'i9)wcfth2$)-ggdx2n-z9ek4o4o759cpgo)_gk(oen8713g%to'

from .base import *


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
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
# ---- disable debug toolbar due to executemany() error when using sqlite3 ----
#
# ["TypeError: not enough arguments for format string" on executemany() INSERT in sqlite3 #1069]
# https://github.com/jazzband/django-debug-toolbar/issues/1069
#

INSTALLED_APPS.remove('debug_toolbar')
MIDDLEWARE.remove('debug_toolbar.middleware.DebugToolbarMiddleware')


#
# http://whitenoise.evans.io/en/stable/django.html#using-whitenoise-in-development
#

INSTALLED_APPS = ['whitenoise.runserver_nostatic'] + INSTALLED_APPS  # put at top, before 'django.contrib.staticfiles'

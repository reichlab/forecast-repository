from .base import *


DEBUG = True

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
    'default': {
        'URL': 'redis://localhost:6379/0',
        'DEFAULT_TIMEOUT': 360,
    },
}

#
# ---- djangorestframework-jwt config ----
#

JWT_AUTH = {
    'JWT_VERIFY_EXPIRATION': False,  # dangerous
}

#
# ---- disable debug toolbar due to executemany() error when using sqlite3 ----
#
# ["TypeError: not enough arguments for format string" on executemany() INSERT in sqlite3 #1069]
# https://github.com/jazzband/django-debug-toolbar/issues/1069
#

INSTALLED_APPS.remove('debug_toolbar')
MIDDLEWARE.remove('debug_toolbar.middleware.DebugToolbarMiddleware')

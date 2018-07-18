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

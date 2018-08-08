from .base import *


DEBUG = True

SECRET_KEY = 'i9)wcfth2$)-ggdx2n-z9ek4o4o759cpgo)_gk(oen8713g%to'

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

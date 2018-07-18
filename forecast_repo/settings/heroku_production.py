import dj_database_url

from .base import *


DEBUG = False

TEST_RUNNER = 'gettingstarted.heroku_test_runner.HerokuDiscoverRunner'

MIDDLEWARE.extend([
    # Simplified static file serving.
    # https://warehouse.python.org/project/whitenoise/
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
]
)

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

redis_url = os.environ.get('REDIS_URL')

if not redis_url:
    raise RuntimeError('heroku_production.py: REDIS_URL not configured!')

RQ_QUEUES = {
    'default': {
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
# ---- static files config ----
#

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.8/howto/static-files/

STATIC_ROOT = os.path.join(PROJECT_ROOT, 'staticfiles')

# Extra places for collectstatic to find static files.
STATICFILES_DIRS = (
    os.path.join(PROJECT_ROOT, 'static'),
)

# Simplified static file serving.
# https://warehouse.python.org/project/whitenoise/
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

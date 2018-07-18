web: newrelic-admin run-program gunicorn forecast_repo.wsgi --log-file=-
worker: python3 manage.py rqworker default

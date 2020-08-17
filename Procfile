web: gunicorn forecast_repo.wsgi --log-file=-
worker: python3 manage.py rqworker high default low

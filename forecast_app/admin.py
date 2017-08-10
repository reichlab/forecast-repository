from django.contrib import admin

from forecast_app.models import DataFile, Project, TimeZero, Target, ForecastModel, Forecast

# Minimal registration of Models.
admin.site.register(DataFile)
admin.site.register(Project)
admin.site.register(TimeZero)
admin.site.register(Target)
admin.site.register(ForecastModel)
admin.site.register(Forecast)

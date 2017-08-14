from django.contrib import admin

from forecast_app.models import DataFile, Project, TimeZero, Target, ForecastModel, Forecast


#
# minimal registration of Models
#

admin.site.register(DataFile)
# admin.site.register(Project)
admin.site.register(TimeZero)
admin.site.register(Target)
admin.site.register(ForecastModel)
admin.site.register(Forecast)


#
# Project admin
#

class ForecastModelInline(admin.TabularInline):
    # todo list of links to edit pages - http://127.0.0.1:8000/admin/forecast_app/forecastmodel/7/change/
    model = ForecastModel
    fields = ['name', 'description', 'url']
    extra = 0

class TargetInline(admin.TabularInline):
    model = Target
    extra = 0

class TimeZeroInline(admin.TabularInline):
    model = TimeZero
    extra = 0


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    # list_display = ('title', 'author', 'display_genre')
    inlines = [ForecastModelInline, TargetInline, TimeZeroInline]

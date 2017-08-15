from django.contrib import admin
from django.contrib.admin.widgets import AdminTextareaWidget

from forecast_app.models import DataFile, Project, TimeZero, Target, ForecastModel, Forecast

#
# minimal registration of Models
#

admin.site.register(DataFile)
# admin.site.register(Project)
admin.site.register(TimeZero)
admin.site.register(Target)
# admin.site.register(ForecastModel)
admin.site.register(Forecast)


#
# Project admin
#

class ForecastModelInline(admin.TabularInline):
    # todo list of links to edit pages - http://127.0.0.1:8000/admin/forecast_app/forecastmodel/7/change/
    model = ForecastModel
    fields = ('name', 'description', 'url')
    extra = 0


class TargetInline(admin.TabularInline):
    model = Target
    extra = 1


class TimeZeroInline(admin.TabularInline):
    model = TimeZero
    extra = 0


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    inlines = [ForecastModelInline, TargetInline, TimeZeroInline]

    def get_form(self, request, obj=None, **kwargs):
        # make the description widget larger
        form = super(ProjectAdmin, self).get_form(request, obj, **kwargs)
        form.base_fields['description'].widget = AdminTextareaWidget()
        return form


#
# ForecastModel admin
#

class ForecastInline(admin.TabularInline):
    model = Forecast
    extra = 1


@admin.register(ForecastModel)
class ForecastModelAdmin(admin.ModelAdmin):
    inlines = [ForecastInline]
    fields = ('project', 'name', 'description', 'url', 'auxiliary_data')

    def get_form(self, request, obj=None, **kwargs):
        # make the description widget larger
        form = super(ForecastModelAdmin, self).get_form(request, obj, **kwargs)
        form.base_fields['description'].widget = AdminTextareaWidget()
        return form

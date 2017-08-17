from django.contrib import admin
from django.contrib.admin.widgets import AdminTextareaWidget
from django.urls import reverse
from django.utils.html import format_html

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

class ForecastModelAdminLinkInline(admin.TabularInline):
    model = ForecastModel
    fields = ('admin_link',)
    readonly_fields = ('admin_link',)
    can_delete = False
    extra = 0


    def admin_link(self, instance):
        url = reverse('admin:{}_{}_change'.format(instance._meta.app_label, instance._meta.model_name),
                      args=(instance.id,))
        return format_html('<a href="{}">{}</a>', url, str(instance))


    # https://stackoverflow.com/questions/4143886/django-admin-disable-the-add-action-for-a-specific-model
    def has_add_permission(self, request):
        return False


class TargetInline(admin.TabularInline):
    model = Target
    extra = 1


class TimeZeroInline(admin.TabularInline):
    model = TimeZero
    ordering = ('timezero_date',)
    extra = 0


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    # inlines = [ForecastModelInline, TargetInline, TimeZeroInline]
    inlines = [ForecastModelAdminLinkInline, TargetInline, TimeZeroInline]


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
    ordering = ('time_zero__timezero_date',)
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

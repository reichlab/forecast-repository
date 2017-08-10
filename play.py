from forecast_app.models import Project, DataFile, ForecastDate, Target, ForecastModel, Forecast

#
# print all user objects
#

for model_class in [DataFile, Project, ForecastDate, Target, ForecastModel, Forecast]:
    print(model_class, model_class.objects.all())

#
# delete all user objects
#

for model_class in [DataFile, Project, ForecastDate, Target, ForecastModel, Forecast]:
    model_class.objects.all().delete()

#
# populate the database programmatically
#

df = DataFile.objects.create(
    location='https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/data-raw',
    file_type='z')  # todo s/b zip file

p = Project.objects.create(
    name='CDC Flu challenge (2016-2017)',
    description='Code, results, submissions, and method description for the 2016-2017 CDC flu contest submissions based on ensembles.',
    url='https://github.com/reichlab/2016-2017-flu-contest-ensembles',
    core_data=df[0],
)

fd = ForecastDate.objects.create()

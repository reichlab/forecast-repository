# Forecast Repository

This is a Django project to implement a repository of forecast challenges. See
[Forecast repository notes](https://docs.google.com/document/d/1cKQY0tgSR8QkxvJUEuMR1xBCvzNYBnMhkNYgK3hCOsk) for
a more detailed description.


# Object creation workflow
Here are the use cases this prototype is to support:

1. The challenge host (project owner):
    - creates a Project and fills in metadata (name, description, url)
    - sets core_data
    - creates Targets
    - creates TimeZeros
1. A competitor (project participant):
    - creates a ForecastModel for the Project and fills in metadata (name, description, url)
    - sets auxiliary_data
    - creates Forecasts
1. Anyone (anonymous):
    - browse objects
    - run visualization app: "Another option would be to create a summary comparison report similar to what flusight does for a single season. A little table of mean absolute error and log score for each model-target comparison." - https://reichlab.slack.com/messages/C57HNDFN0/


# TODO

## code
- model constraints like null=True
- change __str__()s to be prettier
- change app name from forecast_app to something better?


## admin
- Project: ForecastModels inline: should just be a list (inline column?) of links to related ForecastModels' admin edit
  pages
- xx


## apps
- add error visualization views:
  > Another option would be to create a summary comparison report similar to what flusight does for a single season. 
    A little table of mean absolute error and log score for each model-target comparison. 
    via: https://reichlab.slack.com/messages/C57HNDFN0/ 



# Questions

## admin/ux
- What's the object creation workflow? Project: host creates ForecastModel (name, ~description, ~url) and owner
  continues edits?
- xx

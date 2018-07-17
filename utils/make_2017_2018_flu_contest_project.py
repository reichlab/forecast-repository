from pathlib import Path

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.make_cdc_flusight_ensemble_project import _make_cdc_flusight_project


@click.command()
@click.argument('component_models_dir', type=click.Path(file_okay=False, exists=True))
@click.option('--make_project', is_flag=True, default=False)
@click.option('--load_data', is_flag=True, default=False)
def make_2017_2018_flu_contest_project_app(component_models_dir, make_project, load_data):
    """
    Very similar to make_cdc_flusight_ensemble_project.py but with different project meta info.

    NB: _make_cdc_flusight_project() arbitrarily chooses a model subdir to pull Project TimeZeros from, which assumes
    they are all the same. However, in this Project's case, they are not all the same b/c the 'kde-region' model has
    TimeZeros for the entire season calculated ahead of time. That's why it's important that model-id-map.csv puts
    that model's info anywhere but first. O/w we would have many TimeZeros with no forecasts.
    """
    project_name = '2017-2018 CDC Flu contest'
    project_description = 'Code and submissions for 2017-2018 CDC flu prediction contest'
    home_url = 'https://github.com/reichlab/2017-2018-cdc-flu-contest'
    logo_url = 'http://reichlab.io/assets/images/logo/nav-logo.png'
    core_data = 'https://github.com/reichlab/2017-2018-cdc-flu-contest/tree/master/inst/submissions'
    _make_cdc_flusight_project(component_models_dir, make_project, load_data, project_name, project_description,
                               home_url, logo_url, core_data,
                               Path('utils/ensemble-truth-table-script/truths-2017-2018-reichlab.csv'))


if __name__ == '__main__':
    make_2017_2018_flu_contest_project_app()

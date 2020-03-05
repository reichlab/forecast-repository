from pathlib import Path

import click
import shutil


@click.command()
@click.argument('submissions_kot_stable_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('output_dir', type=click.Path(file_okay=False, exists=True))
def split_kot_models_app(submissions_kot_stable_dir, output_dir):
    click.echo("split_kot_models_app: submissions_kot_stable_dir={}, output_dir={}"
               .format(submissions_kot_stable_dir, output_dir))
    split_kot_models(submissions_kot_stable_dir, output_dir)
    click.echo("done")


def split_kot_models(submissions_kot_stable_dir, output_dir):
    """
    A utility that splits out (i.e., copies and renames) the separate KCDE, SARIMA, and ensemble model predictions from
    [1] into separate directories, renaming to obey the CDC Flu Challenge filename format. Note that the KDE model
    predictions [2] are already in that format, and so can be manually copied to the output dir. We rename files like
    this:
    
    - submissions_kot_stable_dir: /Users/cornell/IdeaProjects/2016-2017-flu-contest-ensembles/inst/submissions/submissions-KoT-stable
        EW43-KoTstable-2016-11-09.csv            # ensemble.  EW43-KoTstable-2016-11-09.csv (already standard name format)
        kcde-predictions-2016-43.csv	         # KCDE.   -> EW43-KoTkcde-2016-43.csv
        sarima-predictions-2016-43.csv	         # SARIMA. -> EW43-KoTsarima-2016-43.csv
        
    - output_dir: /Users/cornell/IdeaProjects/split_kot_models_from_submissions
        ensemble/  # EW43-KoTstable-2016-11-09.csv
        kcde/      # kcde-predictions-2016-43.csv -> EW43-KoTkcde-2016-43.csv
      x kde/       # created manually
        sarima/    # sarima-predictions-2016-43.csv -> EW43-KoTsarima-2016-43.csv
    
    [1] https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/inst/submissions/submissions-KoT-stable
    [2] https://github.com/reichlab/2016-2017-flu-contest-ensembles/tree/master/inst/submissions/kde-files
    
    :param submissions_kot_stable_dir: unit of cloned [1] dir
    :param output_dir: dir containing three subdirs (ensemble, kcde, and sarima) that will hold split output predictions
    """
    submissions_kot_stable_dir = Path(submissions_kot_stable_dir)
    output_dir = Path(output_dir)
    for mmwr_year_week_num_path in submissions_kot_stable_dir.iterdir():  # a Path
        if not mmwr_year_week_num_path.is_dir():
            print('skipping', mmwr_year_week_num_path)
            continue

        ensemble_file = list(mmwr_year_week_num_path.glob('EW*.csv'))[0]
        kcde_file = list(mmwr_year_week_num_path.glob('kcde*.csv'))[0]
        sarima_file = list(mmwr_year_week_num_path.glob('sarima*.csv'))[0]

        kcde_file_name = ensemble_file.name.replace('KoTstable', 'KoTkcde')
        sarima_file_name = ensemble_file.name.replace('KoTstable', 'KoTsarima')

        shutil.copy(str(ensemble_file), str(output_dir / 'ensemble'))
        shutil.copy(str(kcde_file), str(output_dir / 'kcde' / kcde_file_name))
        shutil.copy(str(sarima_file), str(output_dir / 'sarima' / sarima_file_name))


if __name__ == '__main__':
    split_kot_models_app()

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from utils.make_thai_moph_project import THAI_PROJECT_NAME
from utils.make_minimal_projects import MINIMAL_PROJECT_NAMES, DOCS_PROJECT_NAME

from django.contrib.auth.models import User
from forecast_app.models import Project


@click.command()
def fix_owners_app():
    """
    Goes hand-in-hand with make_minimal_projects.py and make_thai_moph_project.py (our 'turnkey' projects), this app
    sets project owners and model owners those projects.
    """
    click.echo("fix_owners_app(): starting")

    # delete test projects
    click.echo("* deleting test projects")
    for project_name in MINIMAL_PROJECT_NAMES:
        project = Project.objects.filter(name=project_name).first()
        if not project:
            click.echo(f"  x couldn't find project: '{project_name}'")
            continue

        project.delete()
        click.echo(f"  - deleted project: {project}")

    # delete test users
    click.echo("* deleting test users")
    for user_name in ['project_owner1', 'model_owner1']:
        user = User.objects.filter(username=user_name).first()
        if not user:
            click.echo(f"  x couldn't find test user '{user_name}'")
            continue

        user.delete()
        click.echo(f"  - deleted user: {user}")

    # change project and model owners to nick and cornell. todo 'nectec'?
    for project_name in [THAI_PROJECT_NAME, DOCS_PROJECT_NAME]:
        project_owner_name = 'nick'
        model_owner_name = 'cornell'
        click.echo(f"* project='{project_name}', project_owner_name='{project_owner_name}', "
                   f"model_owner_name={model_owner_name}")
        project = Project.objects.filter(name=project_name).first()
        if not project:
            click.echo(f"  x couldn't find project: '{project_name}'")
            continue

        click.echo(f"** setting project owner: {project_owner_name}")
        project_owner = User.objects.filter(username=project_owner_name).first()
        if not project_owner:
            click.echo(f"  x couldn't find project owner. project_owner_name={project_owner_name}'")
            continue

        old_project_owner = project.owner
        project.owner = project_owner
        project.save()
        click.echo(f"  - changed project owner: {project}: {old_project_owner} -> {project.owner}")

        click.echo(f"** adding project model owners. from: {project.model_owners} to: {model_owner_name}")
        model_owner = User.objects.filter(username=model_owner_name).first()
        if not model_owner:
            click.echo(f"  x couldn't find model owner: model_owner_name='{model_owner_name}'")
            continue

        project.model_owners.add(model_owner)
        click.echo(f"  - added model owners: {project} -> {project.model_owners.all()}")

        click.echo(f"** setting individual model owners. from: {project.model_owners} to: {model_owner_name}")
        for forecast_model in project.models.all():
            old_model_owner = forecast_model.owner
            forecast_model.owner = project_owner
            forecast_model.save()
            click.echo(f"  - set model owner: {forecast_model}: {old_model_owner} -> {forecast_model.owner}")

        project.save()

    # done
    click.echo("fix_owners_app(): done")


if __name__ == '__main__':
    fix_owners_app()

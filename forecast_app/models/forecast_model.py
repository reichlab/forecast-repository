from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import models
from django.urls import reverse

from forecast_app.models.project import Project
from utils.utilities import basic_str


class ForecastModel(models.Model):
    """
    Represents a project's model entry by a competing team, including metadata, model-specific auxiliary data beyond
    core data, and a list of the actual forecasts.
    """
    # from https://github.com/reichlab/covid19-forecast-hub/blob/master/code/validation/accepted-licenses.csv :
    LICENSE_CHOICES = (('afl-3.0', 'Academic Free License v3.0'),
                       ('apache-2.0', 'Apache license 2.0'),
                       ('artistic-2.0', 'Artistic license 2.0'),
                       ('bsl-1.0', 'Boost Software License 1.0'),
                       ('bsd-2-clause', 'BSD 2-clause "Simplified" license'),
                       ('bsd-3-clause', 'BSD 3-clause "New" or "Revised" license'),
                       ('bsd-3-clause-clear', 'BSD 3-clause Clear license'),
                       ('cc', 'Creative Commons license family'),
                       ('cc0-1.0', 'Creative Commons Zero v1.0 Universal'),
                       ('cc-by-nc-4.0', 'Creative Commons Attribution Non-Commercial 4.0'),
                       ('cc-by-4.0', 'Creative Commons Attribution 4.0'),
                       ('cc-by-sa-4.0', 'Creative Commons Attribution Share Alike 4.0'),
                       ('wtfpl', 'Do What The F*ck You Want To Public License'),
                       ('ecl-2.0', 'Educational Community License v2.0'),
                       ('epl-1.0', 'Eclipse Public License 1.0'),
                       ('eupl-1.1', 'European Union Public License 1.1'),
                       ('agpl-3.0', 'GNU Affero General Public License v3.0'),
                       ('gpl', 'GNU General Public License family'),
                       ('gpl-2.0', 'GNU General Public License v2.0'),
                       ('gpl-3.0', 'GNU General Public License v3.0'),
                       ('lgpl', 'GNU Lesser General Public License family'),
                       ('lgpl-2.1', 'GNU Lesser General Public License v2.1'),
                       ('lgpl-3.0', 'GNU Lesser General Public License v3.0'),
                       ('isc', 'ISC'),
                       ('lppl-1.3c', 'LaTeX Project Public License v1.3c'),
                       ('ms-pl', 'Microsoft Public License'),
                       ('mit', 'MIT'),
                       ('mpl-2.0', 'Mozilla Public License 2.0'),
                       ('osl-3.0', 'Open Software License 3.0'),
                       ('postgresql', 'PostgreSQL License'),
                       ('ofl-1.1', 'SIL Open Font License 1.1'),
                       ('ncsa', 'University of Illinois/NCSA Open Source License'),
                       ('unlicense', 'The Unlicense'),
                       ('zlib', 'zLib License'),
                       ('other', 'Other License'))

    owner = models.ForeignKey(User, blank=True, null=True, on_delete=models.SET_NULL,
                              help_text="The model's owner.")
    project = models.ForeignKey(Project, related_name='models', on_delete=models.CASCADE,
                                help_text="The model's project")
    name = models.TextField(help_text="The name of your model in 50 characters or less.")
    is_oracle = models.BooleanField(default=False, help_text="True if this model acts as a truth oracle.")
    abbreviation = models.TextField(help_text="Short name for the model in 15 alphanumeric characters or less. Used in "
                                              "the 'model' column in downloaded CSV score files.")
    team_name = models.TextField(help_text="The name of your team in 50 characters or less.")
    description = models.TextField(help_text="A few paragraphs describing the model. Please see documentation for "
                                             "what should be included here - information on reproducing the model’s "
                                             "results, etc.")
    contributors = models.TextField(default='',
                                    help_text="A list of all individuals involved in the forecasting effort, "
                                              "with affiliations and email address. At least one contributor "
                                              "needs to have a valid email address. The syntax of this field "
                                              "should be: `name1 (affiliation1) <user@address>, name2 "
                                              "(affiliation2) <user2@address2>`")
    license = models.TextField(default='other', choices=LICENSE_CHOICES)
    notes = models.TextField(default='', blank=True, help_text="A catch-all field for arbitrary project use.")
    citation = models.TextField(null=True, blank=True,
                                help_text="A url (DOI link preferred) to an extended description of your model, e.g. "
                                          "blog post, website, preprint, or peer-reviewed manuscript.")
    methods = models.TextField(null=True, blank=True,
                               help_text="An extended description of the methods used in the model. If the model is "
                                         "modified, this field can be used to provide the date of the modification and "
                                         "a description of the change.")
    home_url = models.URLField(help_text="The model's home site.")
    aux_data_url = models.URLField(null=True, blank=True,
                                   help_text="Optional model-specific auxiliary data directory or Zip file containing "
                                             "data files (e.g., CSV files) beyond Project.core_data that were used by "
                                             "this model.")


    def __repr__(self):
        return str((self.pk, self.name, self.abbreviation))


    def __str__(self):  # todo
        return basic_str(self)


    def save(self, *args, **kwargs):
        """
        Validates my abbreviation for uniqueness within my project.
        """
        if (not self.name) or (not self.abbreviation):
            raise ValidationError(f"both name and abbreviation are required. one or both was not found. "
                                  f"name={self.name!r}, abbreviation={self.abbreviation!r}")

        for forecast_model in self.project.models.all():
            if (forecast_model != self) and (self.abbreviation == forecast_model.abbreviation):
                raise ValidationError(f"abbreviation must be unique but was a duplicate of this model: "
                                      f"{forecast_model}. name={self.name!r} abbreviation={self.abbreviation!r}")

        # done
        super().save(*args, **kwargs)


    @classmethod
    def license_name_for_abbreviation(cls, license_abbreviation):
        for choice_abbrev, choice_name in ForecastModel.LICENSE_CHOICES:
            if choice_abbrev == license_abbreviation:
                return choice_name

        return None


    @classmethod
    def is_valid_license_abbreviation(cls, license_abbreviation):
        """
        :return: True if `license_abbreviation` is valid, and False o/w
        """
        return ForecastModel.license_name_for_abbreviation(license_abbreviation)


    def license_as_str(self):
        return ForecastModel.license_name_for_abbreviation(self.license) or ''


    def get_absolute_url(self):
        return reverse('model-detail', args=[str(self.pk)])


    def get_class(self):
        """
        :return: view utility that simply returns a my class as a string. used by delete_modal_snippet.html
        """
        return self.__class__.__name__


    def html_id(self):
        """
        :return: view utility that returns a unique HTML id for this object. used by delete_modal_snippet.html
        """
        return self.__class__.__name__ + '_' + str(self.pk)


    def forecast_for_time_zero(self, time_zero):
        """
        :return: the first Forecast in me corresponding to time_zero. returns None o/w. NB: tests for object equality
        """
        return self.forecasts.filter(time_zero=time_zero).first()

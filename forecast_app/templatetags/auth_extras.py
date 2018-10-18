from django import template
from django.contrib.auth.models import Group


register = template.Library()


#
# https://stackoverflow.com/questions/34571880/how-to-check-in-template-if-user-belongs-to-a-group
#
# In your base.html (template) use the following:
#   {% load auth_extras %}
#
# and to check if the user is in group "moderator":
#   {% if request.user|has_group:"moderator" %}
#     <p>moderator</p>
#   {% endif %}@register.filter(name='has_group')
#
@register.filter
def has_group(user, group_name):
    try:
        group = Group.objects.get(name=group_name)
    except Group.DoesNotExist:
        return False

    return group in user.groups.all()

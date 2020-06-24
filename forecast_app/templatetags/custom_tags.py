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


#
# https://stackoverflow.com/questions/4651172/reference-list-item-by-index-within-django-template/29664945#29664945
#

# in template:
# {% load index %}
# {{ List|index:x }}
#
# It works fine with "for":
# {{ List|index:forloop.counter0 }}

@register.filter
def index(the_list, i):
    return the_list[int(i)]


#
# https://stackoverflow.com/questions/771890/how-do-i-get-the-class-of-a-object-within-a-django-template
#

# in template:
#
# {% load class_tag %}
# {% if Object|get_class == 'AClassName' %}do something{% endif %}
# {{ Object|get_class }}

@register.filter
def get_class(value):
    return value.__class__.__name__

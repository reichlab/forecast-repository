from django import template


register = template.Library()


#
# https://stackoverflow.com/questions/4651172/reference-list-item-by-index-within-django-template/29664945#29664945
#
# A better way: custom template filter: https://docs.djangoproject.com/en/dev/howto/custom-template-tags/
#
# such as get List[x] in templates:
#
# in template
#
# {% load index %}
# {{ List|index:x }}
#
# templatetags/index.py
#
# from django import template
# register = template.Library()
#
# @register.filter
# def index(List, i):
#     return List[int(i)]
#
# if List = [['a','b','c'], ['d','e','f']], you can use {{ List|index:x|index:y }} in template to get List[x][y]
#
# It works fine with "for"
#
# {{ List|index:forloop.counter0 }}
#

@register.filter
def index(the_list, i):
    return the_list[int(i)]

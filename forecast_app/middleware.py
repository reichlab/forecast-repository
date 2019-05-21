import logging

from django.contrib.auth.middleware import get_user
from django.utils.functional import SimpleLazyObject
from rest_framework import exceptions
from rest_framework_jwt.authentication import JSONWebTokenAuthentication


logger = logging.getLogger(__name__)


#
# middleware to solve request.user = AnonymousUser in API view functions - api_views.py
# - https://github.com/GetBlimp/django-rest-framework-jwt/issues/45#issuecomment-255383031
#

class AuthenticationMiddlewareJWT(object):

    def __init__(self, get_response):
        self.get_response = get_response


    def __call__(self, request):
        request.user = SimpleLazyObject(lambda: self.__class__.get_jwt_user(request))
        return self.get_response(request)


    @staticmethod
    def get_jwt_user(request):
        user = get_user(request)
        if user.is_authenticated:
            return user

        jwt_authentication = JSONWebTokenAuthentication()
        if jwt_authentication.get_jwt_value(request):
            try:
                user, jwt = jwt_authentication.authenticate(request)
                return user
            except exceptions.AuthenticationFailed as af:
                logger.warning(request, "get_jwt_user(): AuthenticationFailed: {}.".format(af))
                return user  # AnonymousUser
        else:
            return user  # AnonymousUser

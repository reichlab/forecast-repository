from django.conf import settings
from django.conf.urls import include
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import re_path
from rest_framework_jwt.views import obtain_jwt_token


urlpatterns = [
    # our app and REST API
    re_path(r'', include('forecast_app.urls')),
    re_path(r'^api/', include('forecast_app.api_urls')),
    re_path(r'^admin/', admin.site.urls),  # admin interface - mainly for User management
    re_path(r'^django-rq/', include('django_rq.urls')),
    re_path(r'^api-token-auth/', obtain_jwt_token, name='auth-jwt-get'),  # 'api-jwt-auth'
]

# use static() to add url mapping to serve static files during development (only)
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# debug_toolbar
if settings.DEBUG:
    import debug_toolbar


    urlpatterns += re_path(r'^__debug__/', include(debug_toolbar.urls)),

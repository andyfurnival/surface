"""surface URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import include, path
from django.conf import settings
import os
urlpatterns = [
    path("", include(("theme.urls", "theme"), namespace="surface_theme")),
    path("sbomrepo/", include("sbomrepo.urls")),
    path("scheduler/", include("scheduler.urls")),
    path("", admin.site.urls),
]

SCHEDULING_STRATEGY = getattr(settings, 'SCHEDULING_STRATEGY', os.getenv('SCHEDULING_STRATEGY', 'dkron'))
if SCHEDULING_STRATEGY == 'dkron':
    try:
        urlpatterns.append(path("dkron/", include("dkron.urls")))
    except ImportError:
        # Silently skip if django-dkron isn’t installed, or raise an error if dkron is required
        pass  # Or: raise ImproperlyConfigured("SCHEDULING_STRATEGY='dkron' requires django-dkron")

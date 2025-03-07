from django.urls import path
from . import views

app_name = 'scheduler'

urlpatterns = [
    path('run_all/', views.run_all_tasks, name='run_all_tasks'),
    path('webhook/', views.webhook, name='webhook'),
]
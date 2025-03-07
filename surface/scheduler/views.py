from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponseRedirect
from django.urls import reverse
from .scheduling import get_scheduler
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
import json
from .models import Task
from django.utils import timezone
from django.conf import settings

@staff_member_required
def run_all_tasks(request):
    scheduler = get_scheduler()
    tasks = Task.objects.filter(enabled=True)
    for task in tasks:
        try:
            job_name, job_link = scheduler.run_async(
                'scheduler.run_task',
                task.command.split(),
                scheduler_name=f'task-{settings.AVZONE}-{task.id}-{task.name}-'
            )
            task.last_run_date = timezone.now()
            task.last_run_success = True
            task.save()
        except Exception:
            task.last_run_success = False
            task.save()
    return HttpResponseRedirect(reverse('admin:scheduler_task_changelist'))



@csrf_exempt
@require_POST
def webhook(request):
    """Handle task execution callbacks from schedulers."""
    data = json.loads(request.body.decode('utf-8'))
    token = data.get('token', '')
    if token != settings.SURFACE_WEBHOOK_TOKEN:  # Define in settings.py
        return HttpResponse('Unauthorized', status=401)
    task_name = data.get('task_name', '')  # Match scheduler_name from run_async
    status = data.get('status', '')
    stdout = data.get('stdout', '')  # Adjusted from dKron’s output dict
    stderr = data.get('stderr', '')
    finished_at = data.get('finished_at', None)
    success = status.lower() == 'success'

    try:
        task = Task.objects.get(name=task_name)
        task.last_run_success = success
        task.last_run_date = finished_at or timezone.now()
        task.last_run_stdout = stdout
        task.last_run_stderr = stderr
        task.save()
    except Task.DoesNotExist:
        pass  # Ignore if task not found

    return HttpResponse('OK')
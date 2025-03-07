from django.contrib import admin, messages
from django.utils.html import format_html

from .models import Task
from .scheduling import get_scheduler
from django.utils import timezone
from django.urls import reverse
from django.http import HttpResponseRedirect
from django.conf import settings

@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('name', 'command', 'schedule', 'enabled', 'last_run_date', 'last_run_success', 'notify_on_error', 'retries')
    list_filter = ('enabled', 'last_run_success', 'notify_on_error')
    actions = ['run_task_now', 'disable_tasks', 'enable_tasks']
    change_list_template = "admin/task/change_list.html"
    #change_form_template = "admin/task/change_form.html"  # Optional

    @staticmethod
    def has_dashboard_permission(request):
        return request.user.has_perm('dkron.can_use_dashboard')
    def has_change_permission(self, request, obj=None):
        return request.user.has_perm('dkron.can_change_task')


    def run_task_now(self, request, queryset):
        scheduler = get_scheduler()
        for task in queryset.filter(enabled=True):
            try:
                job_name, job_link = scheduler.run_async(
                    'run_scanner',
                    task.command.split(),
                    scheduler_name=f'task-{settings.AVZONE}-{task.id}-{task.name}-',
                    image=task.image,
                    full_command=True,
                )
                task.last_run_date = timezone.now()
                task.last_run_success = True
                task.save()
                self.message_user(
                    request,
                    format_html(
                        'Scanner {} launching, check log <a href="{}" target="_blank" rel="noopener">here</a>',
                        task.name,
                        job_link,
                    ),
                    level=messages.SUCCESS,
                )

            except Exception as e:
                task.last_run_success = False
                task.save()
                self.message_user(request, f"Error triggering '{task.name}': {str(e)}", level='error')

    run_task_now.short_description = "Run selected tasks now"

    def disable_tasks(self, request, queryset):
        for task in queryset:
            task.enabled = False
            task.save()
    disable_tasks.short_description = "Disable selected tasks"

    def enable_tasks(self, request, queryset):
        for task in queryset:
            task.enabled = True
            task.save()
    enable_tasks.short_description = "Enable selected tasks"

    def save_model(self, request, obj, form, change):
        if not change:
            obj.save()
            return HttpResponseRedirect(reverse('admin:scheduler_task_change', args=[obj.id]))
        super().save_model(request, obj, form, change)




    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['has_dashboard_permission'] = self.has_dashboard_permission(request)
        extra_context['has_change_permission'] = self.has_change_permission(request)
        extra_context['run_all_url'] = reverse('scheduler:run_all_tasks')
        return super().changelist_view(request, extra_context=extra_context)




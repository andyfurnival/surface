from django.db import models

class Task(models.Model):
    name = models.CharField(max_length=255, unique=True, null=False, blank=False)
    description = models.CharField(max_length=255, null=True, blank=True)
    command = models.CharField(max_length=255, null=False, blank=False, help_text="e.g., 'python manage.py run_scanner www.google.com'")
    image = models.CharField(max_length=255, null=True, blank=True,
                             help_text="Container image to run the task (e.g., surface-security:latest)")
    schedule = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Cron spec (e.g., '0 0 * * *') or blank for on-demand"
    )
    enabled = models.BooleanField(default=True)
    last_run_date = models.DateTimeField(null=True, blank=True, editable=False)
    last_run_success = models.BooleanField(null=True, editable=False)
    retries = models.IntegerField(default=0)
    notify_on_error = models.BooleanField(default=True)
    last_run_stdout = models.TextField(null=True, blank=True, editable=False)
    last_run_stderr = models.TextField(null=True, blank=True, editable=False)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Task"
        verbose_name_plural = "Tasks"
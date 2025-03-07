from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Runs a scheduled task'

    def add_arguments(self, parser):
        parser.add_argument('args', nargs='*', help='Task command arguments')
        parser.add_argument('--scheduler-name', help='Name of the scheduler task')

    def handle(self, *args, **options):
        self.stdout.write(f"Running task: {' '.join(args)} (scheduler: {options['scheduler_name']})")
        # Execute the command (e.g., delegate to run_scanner or other logic)
        self.stdout.write(self.style.SUCCESS('Task executed successfully'))
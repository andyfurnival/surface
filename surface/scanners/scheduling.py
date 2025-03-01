from abc import ABC, abstractmethod
import uuid
import subprocess
import threading
from queue import Queue
from django.core.management import call_command
from django.conf import settings
from dkron.utils import run_async as dkron_run_async
import json
import logging

# Import Kubernetes and AWS dependencies
try:
    from kubernetes import client, config
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    client = None  # Define client as None if import fails
    config = None  # Define config as None if import fails

try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    boto3 = None  # Define boto3 as None if import fails


class SchedulingStrategy(ABC):
    @abstractmethod
    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:
        """
        Execute a command asynchronously and return a job identifier and a link/info.
        :param cmd_name: The command to execute (e.g., 'scanner.tasks.scan')
        :param args: Positional arguments for the command
        :param kwargs: Keyword arguments for the command
        :return: Tuple of (job_name, job_link_or_info)
        """
        pass


# dKron Strategy (unchanged)
class DkronSchedulingStrategy(SchedulingStrategy):
    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:
        enable = kwargs.pop('enable', True)
        # Call dkron_run_async
        result = dkron_run_async(cmd_name, *args, enable=enable, **kwargs)

        logger = logging.getLogger(__name__)

        # Add defensive code here (if dKron is unavailable, we don't break the UI, and log the possible error
        if result is None:
            logger.error(f"`dkron_run_async` returned None. Expected a tuple. Args: {cmd_name}, {args}, {kwargs}")
            raise Exception("dkron_run_async returned None")

        # Ensure result is a tuple with two elements
        if not isinstance(result, (tuple, list)) or len(result) != 2:
            logger.error(
                f"`dkron_run_async` returned an invalid result: {result}. Expected a tuple with two elements."
            )
            raise Exception("dkron_run_async returned an invalid result")

    # Unpack the result safely
        job_name, job_link = result

        # Return unpacked values
        return job_name, job_link


# Kubernetes Strategy
class KubernetesSchedulingStrategy(SchedulingStrategy):
    def __init__(self):
        self.deployed = getattr(settings, 'DEPLOYED', False) and KUBERNETES_AVAILABLE
        self.api = None
        if self.deployed and config is not None and client is not None:  # Check module availability
            try:
                config.load_incluster_config()
            except Exception:
                config.load_kube_config()
            self.api = client.BatchV1Api()

    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:
        job_name = f"k8s-{uuid.uuid4().hex[:8]}"

        if not self.deployed or self.api is None:  # Fallback if kubernetes isn’t available
            cmd = ["python", "manage.py", "run_scanner"] + list(args)
            log_file = f"/tmp/{job_name}.log"
            with open(log_file, "w") as f:
                subprocess.Popen(cmd, stdout=f, stderr=f)
            job_link = f"file://{log_file}"
        else:
            job_manifest = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": job_name},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "scanner",
                                "image": getattr(settings, 'K8S_IMAGE', 'surface-scanner:latest'),
                                "command": ["python", "manage.py", "run_scanner"],
                                "args": list(args),
                            }],
                            "restartPolicy": "Never",
                        }
                    },
                    "backoffLimit": 1,
                }
            }
            self.api.create_namespaced_job(namespace="default", body=job_manifest)
            job_link = f"k8s://jobs/{job_name}"

        return job_name, job_link


# AWS EventBridge Strategy with Fargate (ECS)
class EventBridgeSchedulingStrategy(SchedulingStrategy):
    def __init__(self):
        self.deployed = getattr(settings, 'DEPLOYED', False) and AWS_AVAILABLE
        # Initialize queue if boto3 isn’t available or not deployed
        self.queue = Queue() if boto3 is None or not self.deployed else None
        self.eventbridge = None
        self.ecs = None
        if self.deployed and boto3 is not None:
            self.eventbridge = boto3.client('events')
            self.ecs = boto3.client('ecs')

    def _process_queue(self, cmd_name, *args, **kwargs):
        cmd_parts = cmd_name.split('.')
        call_command(cmd_parts[-1], *args, **kwargs)

    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:
        job_name = f"ecs-{uuid.uuid4().hex[:8]}"

        if not self.deployed or boto3 is None:  # Local mode if not deployed or no boto3
            job_link = f"local-queue://{job_name}"
            self.queue.put((cmd_name, args, kwargs))
            threading.Thread(target=self._process_queue, args=(cmd_name, *args), kwargs=kwargs, daemon=True).start()
        else:
            # Production: Use EventBridge to trigger an ECS Fargate task
            rule_name = f"rule-{job_name}"
            cluster = getattr(settings, 'ECS_CLUSTER', 'surface-cluster')
            task_definition = getattr(settings, 'ECS_TASK_DEFINITION', 'surface-scanner-task:latest')
            subnet_ids = getattr(settings, 'ECS_SUBNET_IDS', [])
            security_group_ids = getattr(settings, 'ECS_SECURITY_GROUP_IDS', [])

            self.eventbridge.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(1 minute)",
                State="ENABLED",
            )
            self.eventbridge.put_targets(
                Rule=rule_name,
                Targets=[{
                    "Id": "1",
                    "Arn": f"arn:aws:ecs:{settings.AWS_REGION}:{settings.AWS_ACCOUNT_ID}:cluster/{cluster}",
                    "RoleArn": getattr(settings, 'ECS_EVENT_ROLE_ARN', 'arn:aws:iam::account-id:role/ecs-event-role'),
                    "EcsParameters": {
                        "TaskDefinitionArn": task_definition,
                        "TaskCount": 1,
                        "LaunchType": "FARGATE",
                        "NetworkConfiguration": {
                            "awsvpcConfiguration": {
                                "Subnets": subnet_ids,
                                "SecurityGroups": security_group_ids,
                                "AssignPublicIp": "ENABLED"
                            }
                        },
                        "PlatformVersion": "LATEST"
                    },
                    "Input": json.dumps({
                        "containerOverrides": [{
                            "name": "scanner",
                            "command": ["python", "manage.py", "run_scanner"] + list(args)
                        }]
                    })
                }]
            )
            job_link = f"aws://ecs/tasks/{job_name}"

        return job_name, job_link


# Strategy selector
STRATEGY_MAP = {
    'dkron': DkronSchedulingStrategy,
    'kubernetes': KubernetesSchedulingStrategy,
    'eventbridge': EventBridgeSchedulingStrategy,
}


def get_scheduler():
    strategy_name = getattr(settings, 'SCHEDULING_STRATEGY', 'dkron')
    strategy_class = STRATEGY_MAP.get(strategy_name, DkronSchedulingStrategy)
    return strategy_class()
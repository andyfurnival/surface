from abc import ABC, abstractmethod
import uuid
import subprocess
import threading
from queue import Queue
from django.core.management import call_command
from django.conf import settings
from utils.docker_utils import get_docker_client
import json
import logging
from django.utils import timezone
import time
import docker.errors

# Import Kubernetes dependencies
try:
    from kubernetes import client, config
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    client = None  # Define client as None if import fails
    config = None  # Define config as None if import fails

# Import AWS dependencies
try:
    import boto3
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    boto3 = None  # Define boto3 as None if import fails

def flatten_args(args):
    """Flatten a list of args, handling nested lists from split()"""
    flat_args = []
    for arg in args:
        if isinstance(arg, (list, tuple)):
            flat_args.extend(flatten_args(arg))
        elif isinstance(arg, str):
            flat_args.append(arg)
    return flat_args

def call_docker(cmd, image, job_name, webhook_url, **kwargs):
    docker_ip = kwargs.pop('docker_ip', getattr(settings, 'DOCKER_IP', 'localhost'))
    docker_port = kwargs.pop('docker_port', getattr(settings, 'DOCKER_PORT', 22375))
    docker_tls = kwargs.pop('docker_tls', getattr(settings, 'DOCKER_TLS', False))

    docker_client = get_docker_client(ip=docker_ip, port=docker_port, tls=docker_tls)
    try:
        scanner_timestamp = int(time.time())
        container_name = f'{job_name}{scanner_timestamp}'
        container = docker_client.containers.get(container_name)

        if container.status == "exited":
            container.start()
        elif container.status != "running":
            raise docker_client.errors.APIError(f"Container {container_name} is in unexpected state: {container.status}")
        # Exec command in running container
        exit_code, (stdout, stderr) = container.exec_run(
            container,
            cmd=[
                "sh", "-c",
                f"{cmd}"
                # f" && curl -X POST -H 'Content-Type: application/json' "
                # f"-d '{{\"task_name\": \"{container_name}\", \"status\": \"success\", "
                # f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                # f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"token\": \"{settings.SURFACE_WEBHOOK_TOKEN}\"}}' "
                # f"{webhook_url} || "
                # f"curl -X POST -H 'Content-Type: application/json' "
                # f"-d '{{\"task_name\": \"{container_name}\", \"status\": \"failure\", "
                # f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                # f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"token\": \"{settings.SURFACE_WEBHOOK_TOKEN}\"}}' "
                # f"{webhook_url}"
            ]
        )
        container_id = container.id
    except docker.errors.NotFound:
        container = docker_client.containers.run(
            image,
            command=[
                "sh", "-c",
                f"{cmd}"
                # f" && curl -X POST -H 'Content-Type: application/json' "
                # f"-d '{{\"task_name\": \"{container_name}\", \"status\": \"success\", "
                # f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                # f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"token\": \"{settings.SURFACE_WEBHOOK_TOKEN}\"}}' "
                # f"{webhook_url} || "
                # f"curl -X POST -H 'Content-Type: application/json' "
                # f"-d '{{\"task_name\": \"{container_name}\", \"status\": \"failure\", "
                # f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                # f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"token\": \"{settings.SURFACE_WEBHOOK_TOKEN}\"}}' "
                # f"{webhook_url}"
            ],
            volumes={f'/scanners_{ settings.AVZONE }/output/{ job_name }_{ image }/{ scanner_timestamp }/': {
                        'bind': '/output/',
                        'mode': 'rw',
                    }},  # Persist results
            detach=True,
            name=container_name
        )
        job_link = f"file:///output/{container_name}.log"
        return job_link


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



# dKron Strategy
class DkronSchedulingStrategy(SchedulingStrategy):
    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:

        # Import django-dkron dependencies
        try:
            from dkron.utils import run_async as dkron_run_async
            DKRON_AVAILABLE = True
        except ImportError:
            DKRON_AVAILABLE = False
            dkron_run_async = None

        if not DKRON_AVAILABLE:
            raise ImportError("django-dkron is not installed, required for DkronSchedulingStrategy")

        blocklist_kwargs = {"scheduler_name", "image", "docker_ip", "docker_port", "docker_tls"}
        filtered_kwargs = {key: value for key, value in kwargs.items() if key not in blocklist_kwargs}

        # Define blocklist for args (if needed)
        blocklist_args = {""}
        filtered_args = [arg for arg in args if str(arg) not in blocklist_args]


        logger = logging.getLogger(__name__)
        # Call dkron_run_async
        try:
            result = dkron_run_async(cmd_name, *filtered_args,  **filtered_kwargs)
            if result is None:
                logger.error(f"`dkron_run_async` returned None. Expected a tuple. Args: {cmd_name}, {filtered_args}, {filtered_kwargs}")
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
        except Exception as e:
            logger.error(f"Error running dkron_run_async: {e}")

        # Add defensive code here (if dKron is unavailable, we don't break the UI, and log the possible error
        return None, None


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
        if self.deployed and not KUBERNETES_AVAILABLE:
            raise ImportError(
                "kubernetes is not installed, required for KubernetesSchedulingStrategy with DEPLOYED=True")


        job_name = kwargs.pop('scheduler_name', f"k8s-{uuid.uuid4().hex[:8]}")
        webhook_url = f"{settings.SURFACE_WEBHOOK_URL}/scheduler/webhook/"
        flat_args = flatten_args(args)
        cmd = " ".join([cmd_name] + list(flat_args))
        image = kwargs.pop('image', settings.SCHEDULER_TASK_IMAGE)  #
        if not self.deployed or self.api is None:
            job_link = call_docker(cmd, image, job_name, webhook_url,**kwargs)
        else:

            job_manifest = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": job_name},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "task",
                                "image": { image },
                                "command": ["sh", "-c"],
                                "args": [
                                    f"python manage.py {cmd_name} {' '.join(args)} && "
                                    f"curl -X POST -H 'Content-Type: application/json' "
                                    f"-d '{{\"task_name\": \"{job_name}\", \"status\": \"success\", "
                                    f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                                    f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}' {webhook_url} || "
                                    f"curl -X POST -H 'Content-Type: application/json' "
                                    f"-d '{{\"task_name\": \"{job_name}\", \"status\": \"failure\", "
                                    f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                                    f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}' {webhook_url}"
                                ],
                            }],
                            "restartPolicy": "Never",
                        }
                    },
                    "backoffLimit": kwargs.get('retries', 0),
                }
            }
            self.api.create_namespaced_job(namespace="default", body=job_manifest)
            job_link = f"k8s://jobs/{job_name}"

        return job_name, job_link

    def _simulate_webhook(self, task_name, cmd):
        import subprocess
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        success = process.returncode == 0
        import requests
        webhook_url = f"{settings.SURFACE_WEBHOOK_URL}/scheduler/webhook/"
        requests.post(webhook_url, json={
            "task_name": task_name,
            "status": "success" if success else "failure",
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
            "finished_at": timezone.now().isoformat()
        })


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
        task_name = kwargs.get('scheduler_name', '')
        # Simulate webhook for local mode
        threading.Thread(target=self._simulate_webhook, args=(task_name, [cmd_name] + list(args))).start()

    def _simulate_webhook(self, task_name, cmd):
        import subprocess
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        success = process.returncode == 0
        import requests
        webhook_url = f"{settings.SURFACE_WEBHOOK_URL}/scheduler/webhook/"
        requests.post(webhook_url, json={
            "task_name": task_name,
            "status": "success" if success else "failure",
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
            "finished_at": timezone.now().isoformat()
        })

    def run_async(self, cmd_name: str, *args, **kwargs) -> tuple[str, str]:
        job_name = f"ecs-{uuid.uuid4().hex[:8]}"

        if self.deployed and not AWS_AVAILABLE:
            raise ImportError("boto3 is not installed, required for EventBridgeSchedulingStrategy with DEPLOYED=True")
        job_name = kwargs.pop('scheduler_name', f"ecs-{uuid.uuid4().hex[:8]}")
        webhook_url = f"{settings.SURFACE_WEBHOOK_URL}/scheduler/webhook/"
        flat_args = flatten_args(args)
        cmd = " ".join([cmd_name] + list(flat_args))
        image = kwargs.pop('image', settings.SCHEDULER_TASK_IMAGE)  #
        if not self.deployed or boto3 is None:
            job_link = call_docker(cmd, image, job_name, webhook_url,**kwargs)
        else:
            # Production: Use EventBridge to trigger an ECS Fargate task
            rule_name = f"rule-{job_name}"
            cluster = getattr(settings, 'ECS_CLUSTER', 'surface-cluster')
            task_definition = getattr(settings, 'ECS_TASK_DEFINITION', 'surface-scanner-task:latest')
            subnet_ids = getattr(settings, 'ECS_SUBNET_IDS', [])
            security_group_ids = getattr(settings, 'ECS_SECURITY_GROUP_IDS', [])
            webhook_url = f"{settings.SURFACE_WEBHOOK_URL}/scheduler/webhook/"

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
                            "name": "task",
                            "command": [
                                "sh", "-c",
                                f"python manage.py {cmd_name} {' '.join(args)} && "
                                f"curl -X POST -H 'Content-Type: application/json' "
                                f"-d '{{\"task_name\": \"{job_name}\", \"status\": \"success\", "
                                f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                                f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}' {webhook_url} || "
                                f"curl -X POST -H 'Content-Type: application/json' "
                                f"-d '{{\"task_name\": \"{job_name}\", \"status\": \"failure\", "
                                f"\"stdout\": \"$(cat /proc/1/fd/1)\", \"stderr\": \"$(cat /proc/1/fd/2)\", "
                                f"\"finished_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}' {webhook_url}"
                            ]
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
import unittest
from unittest.mock import patch, Mock, MagicMock
from django.test import TestCase
from scanners.scheduling import (
    DkronSchedulingStrategy,
    KubernetesSchedulingStrategy,
    EventBridgeSchedulingStrategy,
    get_scheduler,
)
from scanners.models import Scanner, ScannerImage, Rootbox


class TestSchedulingStrategies(TestCase):
    def setUp(self):
        self.cmd_name = 'scanners.run_scanner'
        self.args = ['www.google.com']
        self.kwargs = {'scanner': 'scanner_name'}

        scanner_image = ScannerImage.objects.create(name='docker_image', image='registry.com/test/docker_image')
        rootbox = Rootbox.objects.create(
            name='testvm', ip='1.1.1.1', ssh_user='yourmom', location='local', dockerd_tls=False
        )
        Scanner.objects.create(
            image=scanner_image,
            rootbox=rootbox,
            scanner_name='scanner_name',
            input='some_input'
        )

    @patch('scanners.scheduling.dkron_run_async')
    def test_dkron_strategy(self, mock_dkron_run_async):
        mock_dkron_run_async.return_value = ('dkron-job-123', 'http://dkron/job/123')
        strategy = DkronSchedulingStrategy()
        job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
        self.assertTrue(job_name.startswith('dkron-job'))
        self.assertTrue(job_link.startswith('http://dkron'))
        mock_dkron_run_async.assert_called_once_with(
            self.cmd_name, *self.args, enable=True, **self.kwargs
        )

    @patch('subprocess.Popen')
    def test_kubernetes_strategy_local(self, mock_popen):
        with self.settings(DEPLOYED=False):
            strategy = KubernetesSchedulingStrategy()
            job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
            self.assertTrue(job_name.startswith('k8s-'))
            self.assertTrue(job_link.startswith('file:///tmp/k8s-'))
            mock_popen.assert_called_once()
            call_args = mock_popen.call_args[0][0]
            self.assertEqual(call_args, ['python', 'manage.py', 'run_scanner'] + self.args)

    @patch('scanners.scheduling.KUBERNETES_AVAILABLE', False)
    @patch('subprocess.Popen')
    def test_kubernetes_strategy_deployed_no_module(self, mock_popen):
        with self.settings(DEPLOYED=True, K8S_IMAGE='test-image:latest'):
            strategy = KubernetesSchedulingStrategy()
            self.assertFalse(strategy.deployed)
            job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
            self.assertTrue(job_name.startswith('k8s-'))
            self.assertTrue(job_link.startswith('file:///tmp/k8s-'))
            mock_popen.assert_called_once()
            call_args = mock_popen.call_args[0][0]
            self.assertEqual(call_args, ['python', 'manage.py', 'run_scanner'] + self.args)

    @patch('scanners.scheduling.KUBERNETES_AVAILABLE', True)
    def test_kubernetes_strategy_deployed_with_module(self):
        with self.settings(DEPLOYED=True, K8S_IMAGE='test-image:latest'):
            try:
                from kubernetes import client, config
                kubernetes_available = True
            except ImportError:
                kubernetes_available = False

            strategy = KubernetesSchedulingStrategy()
            self.assertTrue(strategy.deployed)

            if kubernetes_available:
                with patch('kubernetes.client.BatchV1Api') as mock_batch_api:
                    with patch('kubernetes.config.load_kube_config') as mock_load_config:
                        mock_api_instance = mock_batch_api.return_value
                        job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
                        self.assertTrue(job_name.startswith('k8s-'))
                        self.assertTrue(job_link.startswith('k8s://jobs/k8s-'))
                        mock_load_config.assert_called_once()
                        mock_api_instance.create_namespaced_job.assert_called_once()
                        job_manifest = mock_api_instance.create_namespaced_job.call_args[1]['body']
                        self.assertEqual(job_manifest['metadata']['name'], job_name)
                        self.assertEqual(job_manifest['spec']['template']['spec']['containers'][0]['image'],
                                         'test-image:latest')
                        self.assertEqual(job_manifest['spec']['template']['spec']['containers'][0]['command'],
                                         ['python', 'manage.py', 'run_scanner'])
                        self.assertEqual(job_manifest['spec']['template']['spec']['containers'][0]['args'], self.args)
            else:
                with patch('subprocess.Popen') as mock_popen:
                    job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
                    self.assertTrue(job_name.startswith('k8s-'))
                    self.assertTrue(job_link.startswith('file:///tmp/k8s-'))
                    mock_popen.assert_called_once()
                    call_args = mock_popen.call_args[0][0]
                    self.assertEqual(call_args, ['python', 'manage.py', 'run_scanner'] + self.args)

    @patch('threading.Thread')
    @patch('scanners.scheduling.call_command')
    def test_eventbridge_strategy_local(self, mock_call_command, mock_thread):
        with self.settings(DEPLOYED=False):
            strategy = EventBridgeSchedulingStrategy()
            job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
            self.assertTrue(job_name.startswith('ecs-'))
            self.assertTrue(job_link.startswith('local-queue://ecs-'))
            mock_thread.assert_called_once()
            self.assertEqual(strategy.queue.qsize(), 1)
            print("Calling _process_queue...")
            strategy._process_queue(self.cmd_name, *self.args, **self.kwargs)
            print(f"call_command called: {mock_call_command.call_count} times")
            mock_call_command.assert_called_once_with('run_scanner', *self.args, **self.kwargs)

    @patch('scanners.scheduling.AWS_AVAILABLE', True)
    def test_eventbridge_strategy_deployed(self):
        with self.settings(
                DEPLOYED=True,
                ECS_CLUSTER='test-cluster',
                ECS_TASK_DEFINITION='test-task:1',
                ECS_SUBNET_IDS=['subnet-123'],
                ECS_SECURITY_GROUP_IDS=['sg-123'],
                ECS_EVENT_ROLE_ARN='arn:aws:iam::123:role/test-role',
                AWS_REGION='us-east-1',
                AWS_ACCOUNT_ID='123456789012'
        ):
            try:
                import boto3
                boto3_available = True
            except ImportError:
                boto3_available = False

            strategy = EventBridgeSchedulingStrategy()
            self.assertTrue(strategy.deployed)

            if boto3_available:
                with patch('boto3.client') as mock_boto3_client:
                    mock_eventbridge = Mock()
                    mock_ecs = Mock()
                    mock_boto3_client.side_effect = [mock_eventbridge, mock_ecs]
                    job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
                    self.assertTrue(job_name.startswith('ecs-'))
                    self.assertTrue(job_link.startswith('aws://ecs/tasks/ecs-'))
                    mock_eventbridge.put_rule.assert_called_once()
                    mock_eventbridge.put_targets.assert_called_once()
                    target_call = mock_eventbridge.put_targets.call_args[1]
                    self.assertEqual(target_call['Rule'], f'rule-{job_name}')
                    ecs_params = target_call['Targets'][0]['EcsParameters']
                    self.assertEqual(ecs_params['TaskDefinitionArn'], 'test-task:1')
                    self.assertEqual(ecs_params['LaunchType'], 'FARGATE')
                    input_json = target_call['Targets'][0]['Input']
                    import json
                    input_data = json.loads(input_json)
                    self.assertEqual(input_data['containerOverrides'][0]['command'],
                                     ['python', 'manage.py', 'run_scanner'] + self.args)
            else:
                with patch('threading.Thread') as mock_thread:
                    job_name, job_link = strategy.run_async(self.cmd_name, *self.args, **self.kwargs)
                    self.assertTrue(job_name.startswith('ecs-'))
                    self.assertTrue(job_link.startswith('local-queue://ecs-'))
                    mock_thread.assert_called_once()
                    self.assertEqual(strategy.queue.qsize(), 1)

    def test_get_scheduler(self):
        with self.settings(SCHEDULING_STRATEGY='dkron'):
            self.assertIsInstance(get_scheduler(), DkronSchedulingStrategy)
        with self.settings(SCHEDULING_STRATEGY='kubernetes'):
            self.assertIsInstance(get_scheduler(), KubernetesSchedulingStrategy)
        with self.settings(SCHEDULING_STRATEGY='eventbridge'):
            self.assertIsInstance(get_scheduler(), EventBridgeSchedulingStrategy)
        with self.settings(SCHEDULING_STRATEGY='invalid'):
            self.assertIsInstance(get_scheduler(), DkronSchedulingStrategy)


if __name__ == '__main__':
    unittest.main()
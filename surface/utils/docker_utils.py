# surface/utils/docker_utils.py
import logging
import docker
import docker.errors
from docker.models.containers import Container
from typing import Tuple, Union, Optional
import docker.utils
from OpenSSL import SSL
from functools import partial

log = logging.getLogger(__name__)


def container_run(client, *args, **kwargs) -> Tuple[str, Container]:
    """Monkeypatch docker client container.run to return (container_id, Container)"""
    kwargs.setdefault('detach', True)
    container = client.containers.run(*args, **kwargs)
    return container.id, container


def container_exec(client: docker.DockerClient, container: Container, cmd: Union[str, list], **kwargs) -> Tuple[
    int, Tuple[bytes, bytes]]:
    """Replacement for container.exec_run to return (exit_code, (stdout, stderr))"""
    kwargs.setdefault('stdout', True)
    kwargs.setdefault('stderr', True)
    cmd_results = client.api.exec_create(container=container.id, cmd=cmd, **kwargs)
    output = client.api.exec_start(cmd_results['Id'], stream=False)
    exit_code = client.api.exec_inspect(cmd_results['Id'])['ExitCode']
    stderr_split = output.rfind(b'\n')
    stdout = output if stderr_split == -1 else output[0:stderr_split]
    stderr = b'' if stderr_split == -1 else output[stderr_split + 1:]
    return exit_code, (stdout, stderr)


class RootboxDockerClient(docker.DockerClient):
    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except SSL.Error:
            log.warning("SSL verification failed, retrying with verify=False")
            kwargs['tls'].verify = False
            super().__init__(*args, **kwargs)
        # Monkeypatch ContainerCollection.run
        self.containers.run = container_run.__get__(self.containers, docker.models.containers.ContainerCollection)
        # Monkeypatch Container.exec_run with a bound method
        docker.models.containers.Container.exec_run = partial(container_exec, self)


def get_docker_client(ip: Optional[str] = None, port: Optional[int] = None, tls: bool = False):
    kwargs = {}
    if ip and port:
        base_url = f"tcp://{ip}:{port}"
        log.debug(f"Using custom Docker base_url: {base_url}")
        kwargs['base_url'] = base_url
        if tls:
            log.debug("Enabling TLS for Docker client")
            kwargs['tls'] = docker.tls.TLSConfig(verify=True)
    else:
        kwargs = docker.utils.kwargs_from_env()
        log.debug(f"Using environment-derived kwargs: {kwargs}")
        base_url = kwargs.get('base_url', '')
        if base_url.startswith('http+docker://'):
            normalized_url = 'tcp://' + base_url[len('http+docker://'):]
            log.warning(f"Normalized invalid DOCKER_HOST from {base_url} to {normalized_url}")
            kwargs['base_url'] = normalized_url

    try:
        client = RootboxDockerClient(**kwargs)
        client.ping()
        log.debug(f"Docker client initialized with kwargs: {kwargs}")
        return client
    except docker.errors.DockerException as e:
        log.error(f"Failed to initialize Docker client with kwargs {kwargs}: {str(e)}")
        raise
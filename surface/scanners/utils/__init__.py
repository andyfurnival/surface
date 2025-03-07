# surface/scanners/utils/__init__.py
import docker
import docker.errors
from utils.docker_utils import get_docker_client, RootboxDockerClient
from django.db.models.query import QuerySet
from scanners import models


def check_scanners_in_box(box, all=False, sparse=True):
    client = get_docker_client(box.ip, port=box.dockerd_port, tls=box.dockerd_tls)
    for c in client.containers.list(sparse=sparse, all=all):
        name = c.attrs.get('Names', [''])[0].lstrip('/') if sparse else c.name
        yield name, c

def check_scanners(rootboxes=None):
    """
    :param rootboxes: show scanners only for the rootboxes in this list. Default is every rootbox.
    :return: tuple with box name and running scanner list
    """
    if isinstance(rootboxes, QuerySet):
        boxes = rootboxes
    else:
        boxes = models.Rootbox.objects.all()
        if rootboxes:
            if isinstance(rootboxes[0], models.Rootbox):
                # forget that queryset
                boxes = rootboxes
            else:
                boxes = boxes.filter(name__in=rootboxes)

    out = []

    for box in boxes:
        cs = []
        for c_name, c in check_scanners_in_box(box):
            cs.append(
                {
                    'id': c.short_id,
                    'name': c_name,
                    'image': c.attrs.get('Image'),
                    'created_at': c.attrs.get('Created'),
                    'status': c.attrs.get('Status'),
                    'state': c.status,
                }
            )
        out.append((box.name, cs))

    return out
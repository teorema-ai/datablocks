import json
import logging
import os
import shutil
import tempfile

import ray

from datablocks.dataspace import Dataspace


logger = logging.getLogger(__name__)


def get_ray_client(*,
               namespace,
               ray_kwargs=None,
               ray_working_dir_config={},
               shutil_symlinks=False, 
               verbose=False):
    ray_kwargs = None if ray_kwargs is None else ray_kwargs.copy()
    if ray_kwargs is None:
        ray_client = ray.init(ignore_reinit_error=True)
    else:
        if 'src_dataspace' in ray_working_dir_config:
            srcspace = ray_working_dir_config['src_dataspace']
            with tempfile.TemporaryDirectory() as tempdir:
                local_working_dir = os.path.join(tempdir, 'ray_working_dir')
                localspace = Dataspace(local_working_dir)
                if srcspace.protocol is None or srcspace.protocol == 'file':
                    # if 'dest_dataspace' not in ray_working_dir_config, then
                    # this copy is unnecessary and srcspace.path can be used in ray_kwargs
                    # directly, but the extra copy is likely not worth the complication in
                    # code logic
                    logger.debug(f"Copying working_dir from srcspace.path {srcspace.path} to local_working_dir {local_working_dir}")
                    shutil.copytree(srcspace.path, local_working_dir, symlinks=shutil_symlinks)
                else:
                    logger.debug(f"Downloading working_dir from srcspace {srcspace} to localspace {localspace}")
                    Dataspace.copy(srcspace, localspace)

                if 'dest_dataspace' in ray_working_dir_config:
                    destspace = ray_working_dir_config['dest_dataspace']
                    logger.debug(f"Uploading working_dir from localspace {localspace} to destspace {destspace}")
                    Dataspace.copy(localspace, destspace)

                if 'runtime_env' not in ray_kwargs:
                    ray_kwargs['runtime_env'] = {}
                ray_kwargs['runtime_env']['working_dir'] = local_working_dir

                # TODO: does this make sense?  tempdir will get deleted right after this call returns, won't it?
                # Call ray.init()  while tempdir exists
                ray_client = ray.init(namespace=namespace,
                                          allow_multiple=True, # this may not work for all versions of ray
                                          **ray_kwargs)
        else:
            ray_client = ray.init(namespace=namespace,
                                  allow_multiple=True, # this may not work for all versions of ray
                                  **ray_kwargs)
    if verbose:
        print(f"Ray: dashboard_url: {ray_client.dashboard_url}")
    return ray_client


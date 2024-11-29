import asyncio
from contextlib import contextmanager
import copy
import logging
import os.path
import tempfile
import time

from fsspec.spec import AbstractFileSystem
from fsspec.asyn import AsyncFileSystem, sync_wrapper
import fsspec.implementations.local
from fsspec.utils import other_paths
from fsspec.implementations.local import make_path_posix

import pyarrow as pa

from . import config
from . import signature
from .utils import DEPRECATED
from .signature import Signature


# TODO: Support for Cloud buckets and credentials
# TODO: Maby Blobspace and Filespace specializations (tempsubdir only available in Filespace?)
# TODO: Locking is a big value-add across specializations!
# TODO: Make dependent on `server`


logger = logging.getLogger(__name__)


class FS:
    def __init__(self, filesystem):
        self.filesystem = filesystem
        # TODO: uncomment this and comment-out/remove 'def get()' below,
        # TODO: after fixing the deadlock error in '_get'
        #self._add_async_method("get")

    def __getattr__(self, key):
        attr = getattr(self.filesystem, key)
        return attr

    def _add_async_method(self, method_name):
        async_method_name = "_" + method_name
        method = sync_wrapper(getattr(self, async_method_name), obj=self)
        setattr(self, method_name, method)
        if not method.__doc__:
            method.__doc__ = getattr(
                getattr(AbstractFileSystem, method_name, None), "__doc__", ""
            )

    def get(self, rpath, lpath, *, recursive=False):
        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = self.expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        _ = [
                self.get_file(rpath, lpath)
                for lpath, rpath in zip(lpaths, rpaths)
                if self.exists(rpath) and not self.isdir(rpath)
            ]
        return _

    async def _get(self, rpath, lpath, *, recursive=False):
        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = await self._expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        _ = await asyncio.gather(
            *[
                self._get_file(rpath, lpath)
                for lpath, rpath in zip(lpaths, rpaths)
                if not self.isdir(rpath)
            ]
        )
        return _


## TODO:  add methods:
##     - put/get
##     - pub/sub/read (sub means "consume one message", read menas "consume all messages without expunging")
# TODO: make 'localhost://<path>' non-pickleable?
#TODO: #REMOVE: pic
class Dataspace:
    signature = Signature(('url',), {})
    def __init__(self,
                 url,
                 *,
                 storage_options={},
                 pic=False, 
    ):
        self.__setstate__((url, storage_options, pic))

    def clone(self, 
              url=None,
              *,
              storage_options=None,
              pic=False,
    ):
        url = url or self.url
        kwargs = dict(storage_options=storage_options or self.storage_options, pic=pic,
        )   
        clone = Dataspace(url, **kwargs)
        return clone

    def with_pic(self, pic=True):
        return self.clone(pic=pic)

    def __delete__(self):
        if isinstance(self._temporary, tempfile.TemporaryDirectory):
            try:
                self._temporary.cleanup()
            except:
                pass
            del self._temporary

    @staticmethod
    def temporary(prefix=None):
        _temporary = tempfile.TemporaryDirectory(prefix=prefix)
        tempspace = Dataspace(_temporary.name)
        tempspace._temporary = _temporary
        return tempspace

    @property
    def fs(self):
        fs = FS(self.filesystem)
        return fs

    @staticmethod
    def filesystem_protocol(filesystem):
        protocol = filesystem.protocol
        if isinstance(protocol, tuple):
            protocol = protocol[0] # e.g., 'sftp' becomes ('sftp', 'ssh') for some reason
        return protocol

    @staticmethod
    def filesystem_url(filesystem, path):
        protocol = Dataspace.filesystem_protocol(filesystem)
        if protocol == 'file':
            filesystem_url = path
        else:
            filesystem_url = f"{protocol}://{path}"
        return filesystem_url

    @staticmethod
    def from_filesystem(filesystem):
        protocol = Dataspace.filesystem_protocol(filesystem)
        url = f"{protocol}://" if protocol != 'file' else ""
        return Dataspace(url, storage_options=filesystem.storage_options)

    @staticmethod
    def from_filesystem_info(info):
        protocol = info['protocol']
        path = info['path']
        storage_options = info['storage_options']
        url = f"{protocol}://{path}" if protocol != 'file' else path
        dataspace = Dataspace(url, storage_options=storage_options)
        return dataspace

    def __getstate__(self):
       if self._temporary:
            raise ValueError(f"Cannot __getstate__ of  temporary Dataspace {self}")
       return self.url, self.storage_options, self.pic,

    def __setstate__(self, state):
        self.url, self.storage_options, self.pic, = state
        self._temporary = None
        p = self.url.find('://')
        if p != -1:
            protocol = self.url[:p]
            path = self.url[p+3:]
        else:
            protocol = 'file'
            path = self.url
        self.protocol = protocol

        if self.protocol != 'file':
            logger.debug(f"Dataspace {self.url} using storage_options {self.storage_options}")
            self.filesystem = fsspec.filesystem(protocol=self.protocol, **self.storage_options)
        else:
            self.filesystem = fsspec.filesystem('file')
        self.root = path
        self._path = self.root # DEPRECATE        self._lock = None
        self._locker = None

    def clear_cache(self):
        if hasattr(self.filesystem, 'cached_files'):
            self.filesystem.cached_files[-1].clear()
            self.filesystem.save_cache()

    def filesystem_from_info(self, info):
        protocol = info['protocol']
        storage_options = info['storage_options']
        if protocol != 'file':
            logger.debug(f"filesystem with protocol {protocol} using storage_options {storage_options}")
            filesystem = fsspec.filesystem(protocol=protocol, **storage_options)
        else:
            filesystem = fsspec.implementations.local.LocalFileSystem()
        return filesystem

    def get_filesystem_info(self):
        return {'protocol': self.protocol, 'path': self.path, 'storage_options': self.storage_options}

    @DEPRECATED
    @property
    def path(self):
        return self.root

    def subspace(self, *args):
        filesystem_info = self.get_filesystem_info()
        sub_filesystem_info = copy.deepcopy(filesystem_info)
        sub_filesystem_info['path'] = self.join(filesystem_info['path'], *args)
        subspace = Dataspace.from_filesystem_info(sub_filesystem_info).with_pic(self.pic)
        subspace._temporary = None if self._temporary is None else True
        return subspace

    def ensure(self, path=None):
        if path is None:
            path = self.root
        logger.debug(f"Ensuring path {path} using filesystem {self.filesystem}")
        self.fs.mkdirs(path, exist_ok=True)
        if not self.fs.exists(path):
            path_ = self.join(path, '')
            self.fs.touch(path_)
        return self

    def remove(self, path=None):
        if path is None:
            path = self.root
        if self.exists(path):
            self.filesystem.rm(path, recursive=True)
        return self

    def is_local(self):
        _ = isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem)
        return _

    def __eq__(self, other):
        if not isinstance(other, Dataspace):
            return False
        state = self.__getstate__()
        other_state = other.__getstate__()
        return state == other_state

    def _tag_(self):
        tag = f"{signature.Tagger.ctor_name(self.__class__)}('{self.url}')"
        return tag

    def __str__(self):
        r = f"{signature.Tagger.ctor_name(self.__class__)}('{self.url}')"
        return r

    def __repr__(self):
        r = f"{signature.Tagger.ctor_name(self.__class__)}('{self.url}', storage_options={self.storage_options})"
        return r

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    def dirname(self, path=None):
        if path is None:
            path = self.root
        if isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem):
            dirname = os.path.dirname(path)
        else:
            dirname = '/'.join(path.split('/')[:-1])
        return dirname

    def basename(self, path=None):
        if path is None:
            path = self.root
        if isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem):
            basename = os.path.basename(path)
        else:
            basename = path.split('/')[-1]
        return basename

    def split(self, path=None):
        if path is None:
            path = self.root
        if isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem):
            parts = os.path.split(path)
        else:
            _parts = path.split('/')
            dirname = '/'.join(_parts[:-1])
            basename = _parts[-1]
            parts = dirname, basename
        return parts

    def strip(self, path, *, front=True, back=True):
        if front and path.startswith(self.sep):
            path = path[1:]
        if back and path.endswith(self.sep):
            path = path[:-1]
        return path

    def join(self, *chain):
        if isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem):
            path = os.path.join(*chain)
        else:
            # FIX: move stripping up outside the 'if' clause
            if len(chain) > 0:
                # TODO: use regex to strip repeated separators at the back/front.
                chain = [self.strip(chain[0], front=False, back=True)] + \
                        [self.strip(link) for link in chain[1:]]
            path = '/'.join(chain)
        return path

    def exists(self, path=None):
        if path is None:
            path = self.root
        exists = self.filesystem.exists(path)
        return exists

    @property
    def sep(self):
        if isinstance(self.filesystem, fsspec.implementations.local.LocalFileSystem):
            sep = os.path.sep
        else:
            sep = '/'
        return sep

    @staticmethod
    def copy(srcspace,
             destspace,
             *,
             tempspace=None):
        """
        Recursively copy directory rooted at srcspace.root to
            destspace.root, if srcspace.root.endswith(srcspace.sep)
            or
            destspace.join(destspace.root, srcspace.basename(srcspac.root))
        """

        src_filesystem = srcspace.filesystem
        srcpath_ = srcspace.root

        dest_filesystem = destspace.filesystem
        destpath_ = destspace.root

        if srcpath_.endswith(srcspace.sep):
            srcpath = srcpath_[:-1]
            if not destpath_.endswith(destspace.sep):
                logger.debug(f"Will copy contents of srcpath {srcpath_} onto destpath {destpath_}")
                logger.debug(f"Removing destpath {destpath_}")
                destspace.remove(destpath_)
            else:
                logger.debug(f"Will copy contents of srcpath {srcpath_} into destpath {destpath_}")
            destpath = destpath_
        else:
            srcpath = srcpath_
            if not destpath_.endswith(destspace.sep):
                logger.debug(f"Will copy srcpath {srcpath} onto destpath {destpath_}")
                logger.debug(f"Removing destpath {destpath_}")
                destspace.remove(destpath_)
                destpath = destpath_
            else:
                srcbasename = srcspace.basename(srcpath)
                destpath = destspace.join(destpath_, srcbasename)
                logger.debug(f"Will copy srcpath {srcpath} into destpath {destpath_}: onto {destpath}")

        if src_filesystem == dest_filesystem:
            # TODO: does this work for non-local filesystems?
            src_filesystem.cp(srcpath, destpath, recursive=True)
        else:
            if tempspace is not None:
                logger.debug(f"Using tempspace {tempspace} as TemporaryDirectory prefix")
                tempdir_manager = tempfile.TemporaryDirectory(prefix=tempspace.root)
            else:
                tempdir_manager = tempfile.TemporaryDirectory()
            with tempdir_manager as tempdir:
                logger.debug(f"Using tempdir {tempdir}")
                if isinstance(src_filesystem, fsspec.implementations.local.LocalFileSystem):
                    logger.debug(f"src_filesystem is a LocalFileSystem: using srcpath {srcpath} as locpath")
                    locpath = srcpath
                else:
                    src_dataspace = Dataspace.from_filesystem(src_filesystem)
                    locfile = src_dataspace.basename(srcpath)
                    locpath = os.path.join(tempdir, locfile)
                    logger.debug(f"Downloading srcpath {srcpath} to locpath {locpath} using filesystem {src_filesystem}")

                    srcfs = FS(src_filesystem)
                    srcfs.get(srcpath, locpath, recursive=True)

                if isinstance(dest_filesystem, fsspec.implementations.local.LocalFileSystem):
                    logger.debug(f"dest_filesystem is a LocalFileSystem: copying locpath {locpath} to destpath {destpath}")
                    dest_filesystem.cp(locpath, destpath, recursive=True)
                else:
                    logger.debug(f"Uploading locpath {locpath} to destpath {destpath} using filesystem {dest_filesystem}")
                    dest_filesystem.put(locpath, destpath, recursive=True)

    @property
    def tag(self):
        return repr(self)

    def __delete__(self):
        self.release()

    @property
    def locker(self):
        from .config import CONFIG
        from .. import db
        if self._locker is None:
            self._locker = db.Locker(user=CONFIG.USER, postgres=CONFIG.POSTGRES)
        return self._locker

    @contextmanager
    def lock(self, *, timeout_millis=-1, need_lock=True):
        self.acquire(timeout_millis=timeout_millis, need_lock=need_lock)
        try:
            yield self
        finally:
            self.release()

    def acquire(self,
                        timeout_millis=-1,
                        need_lock=True):
        lockpath = f"{self.url}"
        lock = self.try_acquire_lock(lockpath,
                                     timeout_millis=timeout_millis,
                                     need_lock=need_lock)
        self._lock = lock
        return self

    def release(self):
        r = False
        if self._lock is not None:
            r = self.release_lock(self._lock)
        self._lock = None
        return r

    def islocked(self):
        locked = self._lock is not None and self._lock.success
        return locked

    def try_acquire_lock(self, path, *, timeout_millis=-1, need_lock=True):
        from ..config import CONFIG
        from ..db import AcquireFailure
        if not need_lock:
            return None
        lockfile = path
        try:
            lock = self.locker.lock(lockfile)
            # HACK: using lock impl
            logger.debug(f"Attempting to acquire lock on path {path} using lock_num {lock.lock_num}")
            # TODO: lock_file --> path
            lock.lock_file = lockfile
            lock.path = path
            if timeout_millis < 0:
                success = lock.acquire(acquire_timeout=2147483647)
            elif timeout_millis == 0:
                success = lock.acquire(acquire_timeout=1)
            else:
                success = lock.acquire(acquire_timeout=timeout_millis)
            lock.success = success
            if not success:
                raise AcquireFailure(
                    f"Failed to acquire lock for path {path} after timeout_millis {timeout_millis}"
                f" with USER {CONFIG.USER}, POSTGRES {CONFIG.POSTGRES}")
        except:
            raise AcquireFailure(f"Failed to acquire lock for path {path} after timeout_millis {timeout_millis}")
        logger.debug(f"Acquired lock on path {path}")
        return lock

    def wait_acquire_lock(self, waitfile, *, wait_secs, wait_interval_secs, timeout_millis):
        from .db import AcquireFailure
        t = 0
        if not os.path.isfile(waitfile):
            if wait_secs != 0:
                while True:
                    logger.debug(f"Sleeping for {wait_interval_secs} secs waiting for file {waitfile}")
                    time.sleep(wait_interval_secs)
                    t += wait_interval_secs
                    if os.path.isfile(waitfile):
                            break
                    if 0 < wait_secs < t:
                        break
        if not os.path.isfile(waitfile):
            raise FileNotFoundError(f"No file {waitfile} after wait of {wait_secs} secs")
        try:
            lock = self.try_acquire_lock(waitfile, timeout_millis=timeout_millis)
        except AcquireFailure:
            raise AcquireFailure(f"No lock on file {waitfile} aftere wait of {wait_secs} secs "
                                           f"and timeout of {timeout_millis} millis")
        return lock

    def release_lock(self, lock):
        if lock is None:
            logger.debug(f"Released empty lock")
            return True
        r = lock.release()
        logger.debug(f"Released lock on path {lock.path}")
        return r

    def force_unlock(self, path):
        lock = self.locker.lock(path)
        # HACK: using lock impl
        # TODO: allow lock impl to be used directly by putting it in the db module
        logger.debug(f"Force unlocking of path {path} using lock_num {lock.lock_num}")
        r = lock.force_unlock()
        return r

    def listdir(self, path):
        """
            Returns a list of basenames of files at `path`.
        """
        prefix = path if path.endswith('/') else path+'/'
        infolist = self.filesystem.listdir(path)
        pathlist_ = [info['name'] for info in infolist if 'name' in info]
        _pathlist = [p[len(prefix):] for p in pathlist_]
        pathlist = [p for p in _pathlist if len(p) > 0]
        return pathlist

    def list(self):
        return self.listdir(self.path)

    def isdir(self, path, relative=False):
        if not relative:
            print(f"DEPRECATED: use of Dataspace.isdir() with an absolute path {path}")
            fullpath = path
        else:
            fullpath = self.subspace(path).path
        return self.filesystem.isdir(fullpath)
    
    def isfile(self, path, relative=False):
        if not relative:
            print(f"DEPRECATED: use of Dataspace.isfile() with an absolute path {path}")
            fullpath = path
        else:
            fullpath = self.subspace(path).path
        return self.filesystem.isfile(fullpath)

DATABLOCKS_DATALAKE = Dataspace(config.DATABLOCKS_DATALAKE_URL)
DATABLOCKS_HOMELAKE = Dataspace(config.DATABLOCKS_HOMELAKE_URL)

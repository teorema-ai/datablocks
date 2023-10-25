"""
pytest $HOME/datablocks/datablocks/test/datatypes.py
or 
python $HOME/datablocks/datablocks/test/datatypes.py
"""

from dataclasses import dataclass
import sys

import time

from fsspec import AbstractFileSystem as FileSystem

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import datablocks
import datablocks.datablock
from datablocks import datablock


DEFAULT_ROOT = datablock.Datatype.DEFAULT_ROOT
DEFAULT_FILESYSTEM = datablock.Datatype.DEFAULT_FILESYSTEM


class Datatype(datablock.Datatype):
    VERSION = '0.0.1'

    def path(self, root):
        return root(self.scope)


class PandasDatatype(Datatype):
    def _dataset_filename(self):
        return 'dataset.parquet'
    
    def read(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
        dataset_path = self.path(root) + '/' + self._dataset_filename()
        table = pq.read_table(dataset_path, filesystem=filesystem)
        frame = table.to_pandas()
        return frame

    def valid(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
        pathlist = filesystem.listdir(self.path(root), detail=False)
        filepath = '/'.join([self.path(root), self._dataset_filename()])
        valid = filepath in pathlist
        return valid
    
    def metric(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
        filelist = self.filesystem.listdir(self.path(root))
        dataset_filename = self._dataset_filename()
        metric = 0
        if (dataset_filename in filelist):
            dataset_filepath = '/'.join([self.path(root), dataset_filename])
            dataset_metadata = pq.read_metadata(dataset_filepath, filesystem=filesystem) 
            metric = dataset_metadata.num_rows
        return metric

    
class PandasArray(PandasDatatype):
    @dataclass 
    class SCOPE:
        size: int = 100
    scope: SCOPE = SCOPE()

    @dataclass
    class CFG:
        verbose: bool = False
        build_delay_secs: int = 2
        echo_delay_secs: int = 0
    cfg: CFG = CFG()
    
    def build(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
              
        if self.cfg.verbose:
            print(f"Building a dataframe of size {self.scope.size} with a delay of {self.cfg.build_delay_secs} secs")
        frame = pd.DataFrame({'range': range(self.scope.size)})
        t0 = time.time()
        while True:
            time.sleep(self.cfg.echo_delay_secs)
            dt = time.time() - t0
            print(f"{dt} secs")
            if dt > self.cfg.build_delay_secs:
                break
        if self.verbose:
            print(f"Built a dataframe of size {self.scope.size}")
        datapath = '/'.join([self.path(root), self._dataset_filename()])
        if self.verbose:
            print(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=filesystem)
        if self.verbose:
            print(f"Wrote dataframe to {datapath}")


class PandasMultiplier(PandasDatatype):
    @dataclass 
    class SCOPE:
        input_frame: pd.DataFrame
        multiplier: float = 10.0
    scope: SCOPE = SCOPE()

    def build(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
              
        if self.verbose:
            print(f"Multiplying a dataframe of size {len(self.scope.input_frame)} with a delay of {self.cfg.build_delay_secs} secs")
        frame = self.scope.input_frame*self.scope.multiplier
        t0 = time.time()
        while True:
            time.sleep(self.cfg.echo_delay_secs)
            dt = time.time() - t0
            print(f"{dt} secs")
            if dt > self.cfg.build_delay_secs:
                break
        if self.cfg.verbose:
            print(f"Built a dataframe of size {len(frame)}")
        datapath = '/'.join([self.path(root), self._dataset_filename()])
        if self.cfg.verbose:
            print(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=filesystem)
        if self.cfg.verbose:
            print(f"Wrote dataframe to {datapath}")

class BuildException(RuntimeError):
    pass

class BuildExceptionDatatype(Datatype):
    def build(self, root, filesystem):
        raise BuildException("TestBuildExceptionDatatype")

class ReadException(RuntimeError):
    pass

class ReadExceptionDatatype(Datatype):
    def build(self, root=DEFAULT_ROOT, filesystem=DEFAULT_FILESYSTEM):
        pass

    def read(self):
        raise ReadException("TestReadExceptionDatatype")
    
def test_scope():
    datablocks.datablock.DBK('datablocks.test.datatypes.ReadExceptionDatatype').scope    

def test_build_exception_datatype():
    exc = None
    try:
        dbk = datablocks.datablock.DBK(BuildExceptionDatatype, 'bexc')
        dbk.build()
    except BuildException as e:
        exc = e
    assert isinstance(exc, BuildException)

def test_read_exception_datatype():
    exc = None
    try:
        datablocks.datablock.DBK(ReadExceptionDatatype, 'rexc').read()
    except ReadException as e:
        exc = e
    assert isinstance(exc, ReadException)


def main(*, debug=True):
    
    test_names = [t for t in globals() if t.startswith('test_')]
    for test_name in test_names:   
        test = globals()[test_name]
        if debug:
            print(f"Running test {test.__name__}")
        test()


if __name__ == "__main__":
    #TODO: add ArgumentParser
    main(debug=True)
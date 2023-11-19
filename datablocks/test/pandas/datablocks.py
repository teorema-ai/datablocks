from dataclasses import dataclass

import time

from fsspec import AbstractFileSystem as FileSystem

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import datablocks
import datablocks.dbx
from datablocks.dbx import Datablock



class PandasAbstractDatablock(Datablock):
    FILENAME = 'data.parquet'
    
    def read(self):
        dataset_path = self.path()
        table = pq.read_table(dataset_path, filesystem=self.filesystem)
        frame = table.to_pandas()
        return frame

    
class PandasArray(PandasAbstractDatablock):
    FILENAME = "data.parquet"
    @dataclass 
    class SCOPE:
        size: int = 100

    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=0, **kwargs):
        self.build_delay_secs = build_delay_secs
        self.echo_delay_secs = echo_delay_secs
        super().__init__(*args, **kwargs)

    def valid(self, topic=None):
        return False
    
    def build(self):
        self.print_verbose(f"Building a dataframe of size {self.scope.size} with a delay of {self.build_delay_secs} secs")
        frame = pd.DataFrame({'range': range(self.scope.size)})
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built a dataframe of size {self.scope.size}")
        datapath = self.path()
        self.print_verbose(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=self.filesystem)
        self.print_verbose(f"Wrote dataframe to {datapath}")


class PandasMultiplier(PandasAbstractDatablock):
    @dataclass 
    class SCOPE:
        input_frame: pd.DataFrame
        multiplier: float = 10.0

    def build(self):
        self.print_verbose(f"Multiplying a dataframe of size {len(self.scope.input_frame)} with a delay of {self.build_delay_secs} secs")
        frame = self.scope.input_frame*self.scope.multiplier
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            print(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built a dataframe of size {len(frame)}")
        datapath = '/'.join([self.path(), self._dataset_filename()])
        self.print_verbose(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=self.filesystem)
        self.print_verbose(f"Wrote dataframe to {datapath}")

class BuildException(RuntimeError):
    pass

class BuildExceptionDatablock(Datablock):
    def build(self):
        raise BuildException("TestBuildExceptionDatablock")

class ReadException(RuntimeError):
    pass

class ReadExceptionDatablock(Datablock):
    def build(self):
        pass

    def read(self):
        raise ReadException("TestReadExceptionDatablock")


def test_scope():
    datablocks.dbx.DBX('datablocks.test.datatypes.ReadExceptionDatablock').scope    


def test_build_exception_datatype():
    exc = None
    try:
        dbx = datablocks.dbx.DBX(BuildExceptionDatablock, 'bexc')
        dbx.build()
    except BuildException as e:
        exc = e
    assert isinstance(exc, BuildException)


def test_read_exception_datatype():
    exc = None
    try:
        datablocks.dbx.DBX(ReadExceptionDatablock, 'rexc').read()
    except ReadException as e:
        exc = e
    assert isinstance(exc, ReadException)

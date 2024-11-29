from dataclasses import dataclass
import pdb
import time


from fsspec import AbstractFileSystem as FileSystem

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import datablocks
from datablocks import signature
import datablocks.dbx
from datablocks.dbx import Datablock



class PandasReadableDatablock(Datablock):
    PATHNAME = 'data.parquet'
  
    def read(self, shardscope, shardroot, topic=None):
        dataset_path = self.path(shardscope, shardroot)
        table = pq.read_table(dataset_path, filesystem=self.filesystem)
        frame = table.to_pandas()
        return frame

    
class PandasArray(PandasReadableDatablock):
    PATHNAME = "data.parquet"
    @dataclass 
    class SCOPE:
        size: int = 100

    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=1, **kwargs):
        self.build_delay_secs = build_delay_secs
        self.echo_delay_secs = echo_delay_secs
        super().__init__(*args, **kwargs)
    
    def build(self, scope, root):
        self.print_verbose(f"Building a dataframe of size {scope.size} with a delay of {self.build_delay_secs} secs using root {root}")
        frame = pd.DataFrame({'range': range(scope.size)})
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built a dataframe of size {scope.size}")
        datapath = self.path(scope, root)
        self.print_verbose(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=self.filesystem)
        self.print_verbose(f"Wrote dataframe to {datapath}")
        return frame
    
    @staticmethod
    def summary(frame):
        if frame is None:
            return None
        s = f"frame of len {len(frame)}"
        rs = repr(s)
        return rs


class PandasMultiplier(PandasReadableDatablock):
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
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__)


class BuildExceptionDatablock(Datablock):
    def build(self):
        raise BuildException()


class ReadException(RuntimeError):
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__)


class ReadExceptionDatablock(Datablock):
    def build(self):
        pass

    def read(self):
        raise ReadException()



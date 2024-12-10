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



class PandasReadable(Datablock):
    PATHNAME = 'data.parquet'
  
    def read(self, blockscope, blockroot):
        dataset_path = self.path(blockscope, blockroot)
        table = pq.read_table(dataset_path, filesystem=self.filesystem)
        frame = table.to_pandas()
        return frame

    
class PandasArray(PandasReadable):
    @dataclass 
    class SCOPE:
        size: int = 100

    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=1, **kwargs):
        self.build_delay_secs = build_delay_secs
        self.echo_delay_secs = echo_delay_secs
        super().__init__(*args, **kwargs)

    def build_frame(self, scope):
        return pd.DataFrame({'array': range(scope.size)})

    def build(self, scope, root):
        self.print_verbose(f"Building a dataframe of size {scope.size} with a delay of {self.build_delay_secs} secs using root {root}")
        frame = self.build_frame(scope)
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built a dataframe")
        datapath = self.path(scope, root)
        self.print_verbose(f"Writing dataframe to datapath {datapath}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, datapath, filesystem=self.filesystem)
        self.print_verbose(f"Wrote dataframe to {datapath}")
        return frame

    def read(self, scope, root):
        path = self.path(scope, root)
        table = pq.read_table(path, filesystem=self.filesystem)
        frame = table.to_pandas()[['array']]
        return frame

    def valid(self, scope, root):
        path = self.path(scope, root)
        return self.filesystem.exists(path) and self.filesystem.isfile(path)
    
    @staticmethod
    def summary(frame):
        s = f"frame of len {len(frame)}"
        rs = repr(s)
        return rs

    def check(self, read_frame, scope, root):
        expected_frame = self.build_frame(scope)
        assert expected_frame.equals(read_frame),       \
            f"expected_frame not equal read_frame:\n" + \
            f"expected_frame:\n{expected_frame}\n"    + \
            f"read_frame:\n{read_frame}"
        if self.verbose:
            print(f"CHECK: {self.__class__}: OKAY")


class PandasArrayMultiplier(PandasArray):
    @dataclass 
    class SCOPE:
        input_frame: pd.DataFrame
        multiplier: float = 10.0

    def build_frame(self, scope):
        return scope.input_frame*scope.multiplier

    def build(self, scope, root):
        self.print_verbose(f"Multiplying a dataframe of size {len(scope.input_frame)} with a delay of {self.build_delay_secs} secs")
        frame = self.build_frame(scope)
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            print(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built a dataframe of size {len(frame)}")
        path = self.path(scope, root)
        self.print_verbose(f"Writing dataframe to path {path}")
        table = pa.Table.from_pandas(frame)
        pq.write_table(table, path, filesystem=self.filesystem)
        self.print_verbose(f"Wrote dataframe to path {path}")
        return frame


class PandasArrayBlock(PandasReadable):
    @dataclass 
    class SCOPE:
        class RANGE(tuple):
            pass
    
        class SIZE_RANGE(RANGE):
            singular: str = 'size'

        sizes: SIZE_RANGE = SIZE_RANGE((100, 110, 120, 130, 140))

    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=1, **kwargs):
        self.build_delay_secs = build_delay_secs
        self.echo_delay_secs = echo_delay_secs
        super().__init__(*args, **kwargs)

    def paths(self, blockscope, blockroots):
        assert len(blockscope.sizes) == len(blockroots), \
            f"Mismatch: number of blockscope shards {len(blockscope.sizes)} " + \
            f"does not match the number of blockroots {len(blockroots)}"
        paths = []
        for i, _ in enumerate(blockscope.sizes):
            paths.append(self.path(None, blockroots[i]))
        return paths

    def read(self, blockscope, blockroots):
        frames = []
        validity = self.valid(blockscope, blockroots)
        if self.debug:
            print(f"DEBUG: read: validity: {validity}")
        paths = self.paths(blockscope, blockroots)
        for path, valid in zip(paths, validity):
            if valid:
                table = pq.read_table(path, filesystem=self.filesystem)
                frame = table.to_pandas()[['array']]
                frames.append(frame)
        return frames
    
    def build_frames(self, scope):
        return [pd.DataFrame({'array': range(size)}) for size in scope.sizes]

    def build(self, scope, roots):
        self.print_verbose(f"Building dataframes of sizes {scope.sizes} with a delay of {self.build_delay_secs} secs using roots {roots}")
        frames = self.build_frames(scope)
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built dataframes")
        paths = self.paths(scope, roots)
        for frame, path in zip(frames, paths):
            self.print_verbose(f"Writing dataframe to path {path}")
            table = pa.Table.from_pandas(frame)
            pq.write_table(table, path, filesystem=self.filesystem)
            self.print_verbose(f"Wrote dataframe to {path}")
        return frame

    def valid(self, scope, roots):
        paths = self.paths(scope, roots)
        valid = [
            self.filesystem.exists(path) and self.filesystem.isfile(path)
            for path in paths
        ]
        return valid
    
    @staticmethod
    def summary(frames):
        s = "frames of len: " + ', '.join([f"{len(frame)}" for frame in frames])
        rs = repr(s)
        return rs

    def check_read_result(self, scope, *, result):
        expected_frames = self.build_frames(scope)
        assert len(expected_frames) == len(result), \
            f"Frame list size mismatch: expected: {len(expected_frames)} != " + \
            f"{len(result)}: result"
        for i, (expected_frame, result_frame) in \
            enumerate(zip(expected_frames, result)):
            assert expected_frame.equals(result_frame),       \
                f"expected_frame {i} not equal result_frame {i}:\n" + \
                f"expected_frame:\n{expected_frame}\n"    + \
                f"result_frame:\n{result_frame}"
        if self.verbose:
            print(f"CHECK: {self.__class__}: OKAY")
        return True


class PandasArrayBlockTwoPhase(PandasArrayBlock):
    SEP = 2
    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=1, **kwargs):
        super().__init__(*args, 
                         build_delay_secs=build_delay_secs, 
                         echo_delay_secs=echo_delay_secs,
                         **kwargs
        )
        self.phase = 0

    def build_frames(self, scope, phase: int):
        if phase == 3:
            lo, hi = 0, len(scope.sizes)
        elif phase == 2:
            lo, hi = self.SEP, len(scope.sizes)
        elif phase == 1:
            lo, hi = 0, self.SEP
        else:
            raise ValueError(f"Unknown phase {phase}")
        _sizes = scope.sizes[lo:hi]
        self.print_verbose(f"Building phase {phase} of dataframes of sizes {_sizes} with a delay of {self.build_delay_secs} secs")
        _frames = [pd.DataFrame({'array': range(size)}) for size in _sizes]
        #
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        self.print_verbose(f"Built dataframes: phase {phase}")
        return _frames

    def write_frames(self, _frames, scope, paths: list[str], phase: int):
        if phase == 1:
            lo, hi = 0, self.SEP
        elif phase == 2:
            lo, hi = self.SEP, len(scope.sizes)
        elif phase == 3:
            lo, hi = 0, len(scope.sizes)
        else:
            raise ValueError(f"Unknown phase {phase}")
        _paths = paths[lo:hi]
        for frame, path in zip(_frames, _paths):
            self.print_verbose(f"Writing dataframe to path {path}")
            table = pa.Table.from_pandas(frame)
            pq.write_table(table, path, filesystem=self.filesystem)
            self.print_verbose(f"Wrote dataframe to {path}")

    def build(self, scope, roots):
        paths = self.paths(scope, roots)
        valid = self.valid(scope, roots)
        frames = self.build_frames(scope, phase=self.phase+1)
        self.write_frames(frames, scope, paths, phase=self.phase+1)
        self.phase += 1
        return frames

    def check_read_result(self, scope, *, result):
        check_phase = 1 if self.phase == 1 else self.phase + 1
        expected = self.build_frames(scope, phase=check_phase)
        assert len(expected) == len(result), \
            f"Frame list size mismatch: expected: {len(expected)} != " + \
            f"{len(result)}: read"
        for i, (expected_frame, read_frame) in \
            enumerate(zip(expected, result)):
            assert expected_frame.equals(read_frame),       \
                f"expected_frame {i} not equal read_frame {i}:\n" + \
                f"expected_frame:\n{expected_frame}\n"    + \
                f"read_frame:\n{read_frame}"
        if self.verbose:
            print(f"CHECK: {self.__class__}: OKAY")
    

class PandasArrayBook(PandasReadable):
    TOPICS = {
        'A': 'a.parquet',
        'B': 'b.parquet',
    }

    @dataclass 
    class SCOPE:
        size_a: int = 100
        size_b: int = 200

    def __init__(self, *args, build_delay_secs=2, echo_delay_secs=1, **kwargs):
        self.build_delay_secs = build_delay_secs
        self.echo_delay_secs = echo_delay_secs
        super().__init__(*args, **kwargs)
    
    def build(self, scope, roots):
        self.print_verbose(f"Building dataframes of size {scope.size_a} and {scope.size_b} with a delay of {self.build_delay_secs} secs using roots {roots}")
        t0 = time.time()
        while True:
            time.sleep(self.echo_delay_secs)
            dt = time.time() - t0
            self.print_verbose(f"{dt} secs")
            if dt > self.build_delay_secs:
                break
        frames = {}
        for topic in ['A', 'B']:
            frame = self.build_frame(scope, topic)
            self.print_verbose(f"Built dataframe {topic}")
            datapath = self.path(scope, roots, topic)
            self.print_verbose(f"Writing dataframe to datapath {datapath}")
            table = pa.Table.from_pandas(frame)
            pq.write_table(table, datapath, filesystem=self.filesystem)
            self.print_verbose(f"Wrote dataframe {topic} to {datapath}")
            frames[topic] = frame
        return frames

    def read(self, scope, roots, topic):
        path = self.path(scope, roots, topic)
        return pd.read_parquet(path)

    def valid(self, scope, roots, topic):
        path = self.path(scope, roots, topic)
        return self.filesystem.exists(path) and self.filesystem.isfile(path)

    @staticmethod
    def summary(frames):
        fa, fb = frames['A'], frames['B']
        s = f"frames of lens {len(fa)}, {len(fb)}"
        rs = repr(s)
        return rs

    def build_frame(self, scope, topic):
        size = self.build_size(scope, topic)
        return pd.DataFrame({'range': range(size)})
    
    def build_size(self, scope, topic):
        return getattr(scope, f"size_{topic.lower()}")

    def check(self, read_frame, scope, topic):
        expected_frame = self.build_frame(scope, topic)
        assert expected_frame.equals(read_frame),       \
            f"expected_frame not equal read_frame:\n" + \
            f"expected_frame:\n{expected_frame}\n"    + \
            f"read_frame:\n{read_frame}"
        if self.verbose:
            print(f"CHECK: {self.__class__}: OKAY")


class BuildException(RuntimeError):
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__, [], {})


class BuildExceptionDatablock(Datablock):
    def build(self, scope, roots):
        raise BuildException()


class ReadException(RuntimeError):
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__)


class ReadExceptionDatablock(Datablock):
    def build(self, scope, roots):
        pass

    def read(self, scope, roots):
        raise ReadException()



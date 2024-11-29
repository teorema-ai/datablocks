import os

import datablocks
import datablocks.dataspace
from datablocks.eval.request import Request

from datablocks.eval.request import ArgResponseException
from datablocks.test.datablocks import (
    BuildException, BuildExceptionDatablock,
    ReadException, ReadExceptionDatablock,
)


TESTLAKE = datablocks.dataspace.Dataspace(os.path.join(os.path.dirname(__file__), 'tmp')).remove().ensure()
TEST_FILE_POOL = datablocks.FILE_POOL.clone(dataspace=TESTLAKE, throw=False)
CLEAR = True
DEBUG = False
THROW = True #DEBUG

def _test_build_exception(dbx, topic=None, *, verbose=True, exception_cls):
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.intent.pretty()))
    dbx.build_request().compute()

    e = dbx.show_build_graph().exception
    assert isinstance(e, ArgResponseException) , f"Incorrect exception: type: {type(e)}, {e}"
    assert len(dbx.show_build_graph().traceback) > 0, "Missing traceback"

    _e = dbx.show_build_batch_graph().exception
    assert isinstance(_e, exception_cls) , f"Incorrect exception: type: {type(_e)}, {_e}"
    assert len(dbx.show_build_batch_graph().traceback) > 0, "Missing traceback"
        

def _test(dbx, 
          topic=None, 
          *, 
          build=True, 
          read=False, 
          show=True, 
          check_batch_graph=True, 
          check_batch_exception=False, 
          verbose=False,
          clear=CLEAR, 
          debug=DEBUG,
          throw=THROW,
):
    dbx = dbx.Datablock(verbose=verbose, debug=debug).Databuilder(throw=throw, pool=datablocks.FILE_LOGGING_POOL)
    print()
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.intent.pretty()))
    if build:
        if verbose:
            print(f"extent: pre-build: {dbx}: \n", dbx.extent.pretty())
            print(f"metric: pre-build: {dbx}: \n", dbx.metric.pretty())
        result = dbx.build()
        if verbose:
            print(f"extent: post-build: {dbx}\n", dbx.extent.pretty())
            print(f"metric: post-build: {dbx}: \n", dbx.metric.pretty())
    if read:
        if topic:
            _ = dbx.read(topic)
            if verbose:
                print(_)
        else:
            _ = dbx.read()
            if verbose:
                print(_)
    if show:
        dbx.show_records()
        dbx.show_record_columns()
        dbx.show_record()
        dbx.show_named_record()
        dbx.show_build_batch_count()
        dbx.show_build_graph().transcript
        dbx.show_build_batch_graph().transcript.keys()
        dbx.show_build_scope()
        dbx.show_build_graph()
        dbx.show_build_batch_graph()
        #assert isinstance(dbx.show_build_graph().request, Request)
        
        if verbose:
            print(f"show_records():\n{dbx.show_records()}")
            print(f"show_record_columns():\n{dbx.show_record_columns()}")
            print(f"show_record():\n{dbx.show_record()}")
            print(f"show_named_record():\n{dbx.show_named_record()}")
            print(f"show_build_batch_count():\n{dbx.show_build_batch_count()}")
            print(f"show_build_graph().transcript():\n{dbx.show_build_graph().transcript}")
            print(f"dbx.show_build_batch_graph().transcript.keys():\n{dbx.show_build_batch_graph().transcript.keys()}")
            print(f"show_build_scope():\n{dbx.show_build_scope()}")
            print(f"show_build_graph():\n{dbx.show_build_graph()}")
            print(f"show_build_batch_graph():\n{dbx.show_build_batch_graph()}")

    if check_batch_graph:
        assert dbx.show_build_graph().exception is None, f"Unexpected exception: type: {type(dbx.show_build_graph().exception)}, {dbx.show_build_graph().exception}"
        assert len(dbx.show_build_batch_graph().traceback) == 0, "Unexpected traceback"
        assert len(dbx.show_build_batch_graph().logpath) > 0, "Empty logpath"
        assert not verbose or len(dbx.show_build_batch_graph().log()) > 0, "Empty log"
        assert len(dbx.show_build_batch_graph().result) > 0, "Empty result"

    if clear:
        dbx.UNSAFE_clear()
        if verbose:
            print(f">>> Cleared")


def test_pandas_datablock():
    pandas_datablock = datablocks.DBX('datablocks.test.datablocks.PandasArray', 'pdbk')\
        .Databuilder(dataspace=TESTLAKE)
    _test(pandas_datablock, check_batch_graph=True)

def test_pandas_datablock_verbose():
    pandas_datablock = datablocks.DBX('datablocks.test.datablocks.PandasArray', 'pdbk')\
        .Databuilder(dataspace=TESTLAKE)
    _test(pandas_datablock, check_batch_graph=True, verbose=True)

def test_pandas_build_exception():
    pexc = datablocks.DBX('datablocks.test.datablocks.BuildExceptionDatablock', 'pexc')\
            .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL)
    _test_build_exception(pexc, exception_cls=BuildException)

def test_scope():
    datablocks.dbx.DBX('datablocks.test.datablocks.ReadExceptionDatablock').scope    


def test_build_exception():
    exc = None
    try:
        dbx = datablocks.dbx.DBX(BuildExceptionDatablock, 'bexc')
        dbx.build()
    except BuildException as e:
        exc = e
    assert isinstance(exc, BuildException)


def test_read_exception():
    exc = None
    try:
        datablocks.dbx.DBX(ReadExceptionDatablock, 'rexc').read()
    except ReadException as e:
        exc = e
    assert isinstance(exc, ReadException)

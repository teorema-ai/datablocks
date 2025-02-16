import os
import pdb

import datablocks
import datablocks.dataspace
from datablocks.eval.request import Request

from datablocks.eval.request import ArgResponseException
from datablocks.test.datablocks import (
    BuildException, BuildExceptionDatablock,
    ReadException, ReadExceptionDatablock,
)

BASE_ACTIONS = ('build', 'read', 'show')
CHECK = ('batch_graph', 'batch_exception')
CLEAR = True
DEBUG = True #DEBUG
DEBUG = False
VERBOSE = True #DEBUG
VERBOSE = False
THROW = True #DEBUG
THROW = False
TESTLAKE = datablocks.dataspace.Dataspace(os.path.join(os.path.dirname(__file__), 'tmp')).remove().ensure()
TEST_STDOUT_POOL = datablocks.STDOUT_POOL.clone(dataspace=TESTLAKE, throw=THROW)
TEST_FILE_POOL = datablocks.FILE_POOL.clone(dataspace=TESTLAKE, throw=THROW)
TEST_POOL = TEST_STDOUT_POOL #DEBUG
TEST_POOL = TEST_FILE_POOL



def _test_build_exception(dbx, topic=None, *, verbose=True, exception_cls):
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.intent.pretty()))
    dbx.build_request().compute()

    e = dbx.show_build_graph().exception
    assert isinstance(e, ArgResponseException) , f"Incorrect exception: type: {type(e)}, {e}"
    assert len(dbx.show_build_batch_graph().traceback) > 0, "Missing traceback"

    _e = dbx.show_build_batch_graph().exception
    assert isinstance(_e, exception_cls) , f"Incorrect exception: type: {type(_e)}, {_e}"
    assert len(dbx.show_build_batch_graph().traceback) > 0, "Missing traceback"
        

def _test_read_exception(dbx, topic=None, *, verbose=True, exception_cls):
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.extent.pretty()))
    response = dbx.read_request().evaluate()
    response.result()
    e = response.exception()
    t = response.traceback()
    assert isinstance(e, exception_cls), f"Incorrect exception: type: {type(e)}, {e}"
    assert t is not None, "Missing traceback"


def _test(dbx,  
          *, 
          actions=BASE_ACTIONS,
          phases=1,
          check_batch_graph=True, 
          check_batch_exception=False, 
          check=CHECK,
          verbose=VERBOSE,
          clear=CLEAR, 
          debug=DEBUG,
          throw=THROW,
):
    build = 'build' in actions
    read = 'read' in actions
    show = 'show' in actions
    check_batch_graph = 'batch_graph' in check
    check_batch_exception = 'batch_exception' in check
    
    dbx = dbx.DBX(verbose=verbose, debug=debug,)\
        .Datablock(verbose=verbose, debug=debug)\
        .Databuilder(throw=throw, pool=TEST_POOL, verbose=verbose, debug=debug)
    print()
    for _ in range(phases):
        if verbose:
            print(f"intent: {dbx}:\n", str(dbx.intent.pretty()))
        if build:
            if verbose:
                print(f"extent: pre-build: {dbx}: \n", dbx.extent.pretty())
                print(f"metric: pre-build: {dbx}: \n", dbx.metric.pretty())
            dbx.build()
            if verbose:
                print(f"extent: post-build: {dbx}\n", dbx.extent.pretty())
                print(f"metric: post-build: {dbx}: \n", dbx.metric.pretty())
        if read:
            if debug:
                print(f"DEBUG: dbx.datablock_cls: {dbx.datablock_cls}")
            if hasattr(dbx.datablock_cls, 'TOPICS'):
                for topic in dbx.topics:
                    result = dbx.read(topic)
                    if verbose:
                        print(f"result:\n{result}")
                    if dbx.check_read_result is not None:
                        dbx.check_read_result(topic, result=result,)
            else:
                result = dbx.read()
                if dbx.check_read_result is not None:
                    dbx.check_read_result(dbx.scope, result=result)
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
            print(f"Cleared")


def test_pandas_array_single():
    dbx = datablocks.DBX('datablocks.test.datablocks.PandasArray', 'pandas_array')\
        .Databuilder(dataspace=TESTLAKE,)
    _test(dbx, check_batch_graph=True)


def test_pandas_array_multiplier():
    array = datablocks.DBX('datablocks.test.datablocks.PandasArray', 'pandas_array')\
        .Databuilder(dataspace=TESTLAKE,)
    _test(array, check_batch_graph=True, clear=False)
    multiplier = datablocks.DBX('datablocks.test.datablocks.PandasArrayMultiplier', 'pandas_array_multiplier')\
        .Databuilder(dataspace=TESTLAKE,)\
        .SCOPE(input_frame=array.READ())
    _test(multiplier, check_batch_graph=True)


def test_pandas_array_block_single_phase():
    dbx = datablocks.DBX('datablocks.test.datablocks.PandasArrayBlock', 'pandas_array_block')\
        .Databuilder(dataspace=TESTLAKE,)
    _test(dbx, check_batch_graph=True)


def test_pandas_array_block_two_phase():
    dbx = datablocks.DBX('datablocks.test.datablocks.PandasArrayBlockTwoPhase', 'pandas_array_block_two_phase')\
        .Datablock(verbose=True)\
        .Databuilder(dataspace=TESTLAKE,)
    _test(dbx, check_batch_graph=True, phases=2)


def test_pandas_array_book():
    pandas_array_book_dbx = datablocks.DBX('datablocks.test.datablocks.PandasArrayBook', 'pandas_array_book')\
        .Databuilder(dataspace=TESTLAKE)
    _test(pandas_array_book_dbx, check_batch_graph=True)


def test_build_exception():
    build_exception_datablock = \
        datablocks.DBX('datablocks.test.datablocks.BuildExceptionDatablock', 'build_exception')\
            .Databuilder(dataspace=TESTLAKE, pool=TEST_POOL)
    _test_build_exception(build_exception_datablock, exception_cls=BuildException)


def test_read_exception():
    read_exception_datablock = \
        datablocks.DBX('datablocks.test.datablocks.ReadExceptionDatablock', 'read_exception')\
            .Databuilder(dataspace=TESTLAKE, pool=TEST_POOL)
    _test_read_exception(read_exception_datablock, exception_cls=ReadException)


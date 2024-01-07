import datablocks
import datablocks.dataspace
from datablocks.eval.request import Request

TESTLAKE = datablocks.dataspace.Dataspace.temporary()
TEST_FILE_POOL = datablocks.FILE_POOL.clone(dataspace=TESTLAKE, throw=False)
VERBOSE = True


def _test_exception(dbx, topic=None, *, verbose=True):
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.intent().pretty()))
    response = dbx.build_request().evaluate()
    response.result()
    assert dbx.show_build_graph().exception != 'None', "Missing exception"
    assert len(dbx.show_build_graph().traceback) > 0, "Missing traceback"
        

def _test(dbx, topic=None, *, build=True, read=False, show=True, check_batch_graph=True, check_batch_exception=False, clear=False, verbose=True):
    print()
    if verbose:
        print(f"intent: {dbx}:\n", str(dbx.intent().pretty()))
    if build:
        if verbose:
            print(f"extent: pre-build: {dbx}: \n", dbx.extent().pretty())
            print(f"metric: pre-build: {dbx}: \n", dbx.metric().pretty())
        result = dbx.build()
        if verbose:
            print(f"extent: post-build: {dbx}\n", dbx.extent().pretty())
            print(f"metric: post-build: {dbx}: \n", dbx.metric().pretty())
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
        dbx.show_build_records()
        dbx.show_build_record_columns()
        dbx.show_build_record()
        dbx.show_named_record()
        dbx.show_build_batch_count()
        dbx.show_build_transcript()
        dbx.show_build_scope()
        dbx.show_build_graph()
        dbx.show_build_batch_graph()
        assert isinstance(dbx.show_build_graph().request, Request)
        
        if verbose:
            print(f"show_build_records(): {dbx.show_build_records()}")
            print(f"show_build_record_columns(): {dbx.show_build_record_columns()}")
            print(f"show_build_record(): {dbx.show_build_record()}")
            print(f"show_named_record(): {dbx.show_named_record()}")
            print(f"show_build_batch_count(): {dbx.show_build_batch_count()}")
            print(f"show_build_transcript(): {dbx.show_build_transcript()}")
            print(f"show_build_scope(): {dbx.show_build_scope()}")
            print(f"show_build_graph(): {dbx.show_build_graph()}")
            print(f"show_build_batch_graph(): {dbx.show_build_batch_graph()}")

    if check_batch_graph:
        assert len(dbx.show_build_batch_graph().logpath) > 0, "Empty logpath"
        assert len(dbx.show_build_batch_graph().log()) > 0, "Empty log"
        assert len(dbx.show_build_batch_graph().result) > 0, "Empty result"
        assert dbx.show_build_batch_graph().exception == 'None', "Unexpected exception"
        assert len(dbx.show_build_batch_graph().traceback) == 0, "Unexpected traceback"

    if clear:
        dbx.UNSAFE_clear()
        if verbose:
            print(f">>> Cleared")


MIRLOGCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRLogCoHN', 'mirlogcohn')\
        .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL, verbose=VERBOSE)\
            .Datablock(verbose=VERBOSE)


MIRCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRCoHN', 'mircohn')\
        .SCOPE(logcounts=MIRLOGCOHN.READ())\
        .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL, verbose=VERBOSE)


MIRNA = datablocks.DBX('datablocks.test.micron.datablocks.miRNA', 'mirna')\
    .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL, verbose=VERBOSE)\
    .Datablock(verbose=VERBOSE).SCOPE()


MIR_COSEQS_NPASSES = 10
MIR_COSEQS_SEQS_PER_RECORD = 300
MIRCOSEQSHN = \
        datablocks.DBX('datablocks.test.micron.datablocks.miRCoSeqs', f"mircoseqshn_{MIR_COSEQS_NPASSES}_{MIR_COSEQS_SEQS_PER_RECORD}")\
            .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL, verbose=VERBOSE)\
            .Datablock(verbose=VERBOSE)\
                .SCOPE(logcounts=MIRCOHN.READ('logcounts'), 
                       logcontrols=MIRCOHN.READ('logcontrols'),
                       seqs=MIRNA.READ(), 
                       npasses=MIR_COSEQS_NPASSES, 
                       nseqs_per_record=MIR_COSEQS_SEQS_PER_RECORD)


def test_mirlogcohn():
    _test(MIRLOGCOHN)


def test_mircohn_logcounts():
    _test(MIRLOGCOHN)
    _test(MIRCOHN, 'logcounts')


def test_mircohn_counts():
    _test(MIRLOGCOHN)
    _test(MIRCOHN, 'logcounts')
    _test(MIRCOHN, 'counts', clear=True)


def test_mirna():
    _test(MIRNA, clear=True)


def test_mircoseq():
    _test(MIRNA)
    _test(MIRCOSEQSHN, 'samples', clear=True)
    

def test_pandas_datablock():
    pdbk = datablocks.DBX('datablocks.test.pandas.datablocks.PandasArray', f"pdbk")\
        .Databuilder(dataspace=TESTLAKE)
    _test(pdbk, check_batch_graph=False)


def test_pandas_exception():
    pexc = datablocks.DBX('datablocks.test.pandas.datablocks.BuildExceptionDatablock', 'pexc')\
            .Databuilder(dataspace=TESTLAKE, pool=TEST_FILE_POOL)
    _test_exception(pexc)
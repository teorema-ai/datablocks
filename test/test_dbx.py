import datablocks
import datablocks.dataspace


TESTLAKE = datablocks.dataspace.Dataspace.temporary()


def _test(dbx, topic=None, *, build=True, read=False, show=False, clear=False, verbose=True):
    print()
    if verbose:
        print(f"intent: {dbx}\n", str(dbx.intent()))
    if build:
        if verbose:
            print(f"extent: pre-build: {dbx}: \n", dbx.extent())
            print(f"metric: pre-build: {dbx}: \n", dbx.metric())
        result = dbx.build()
        if verbose:
            print(f"extent: post-build: {dbx}\n", dbx.extent())
            print(f"metric: post-build: {dbx}: \n", dbx.metric())
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

    if clear:
        dbx.UNSAFE_clear()
        if verbose:
            print(f">>> Cleared")


MIRLOGCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRLogCoHN', 'mirlogcohn')\
        .Databuilder(dataspace=TESTLAKE).Datablock(verbose=True)


MIRCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRCoHN', 'mircohn')\
        .SCOPE(logcounts=MIRLOGCOHN.READ())\
        .Databuilder(dataspace=TESTLAKE)


MIRNA = datablocks.DBX('datablocks.test.micron.datablocks.miRNA', 'mirna').Datablock(verbose=True).SCOPE()


MIR_COSEQS_NPASSES = 10
MIR_COSEQS_SEQS_PER_RECORD = 300
MIRCOSEQSHN = \
        datablocks.DBX('datablocks.test.micron.datablocks.miRCoSeqs', f"mircoseqshn_{MIR_COSEQS_NPASSES}_{MIR_COSEQS_SEQS_PER_RECORD}")\
            .Datablock(verbose=True).SCOPE(logcounts=MIRCOHN.READ('logcounts'), 
                                           logcontrols=MIRCOHN.READ('logcontrols'),
                                           seqs=MIRNA.READ(), 
                                           npasses=MIR_COSEQS_NPASSES, 
                                           nseqs_per_record=MIR_COSEQS_SEQS_PER_RECORD)


def test_mirlogcohn():
    _test(MIRLOGCOHN)


def test_mircohn_logcounts():
    _test(MIRCOHN, 'logcounts')


def test_mircohn_counts():
    _test(MIRCOHN, 'counts', clear=True)


def test_mirna():
    _test(MIRNA)


def test_mircoseq():
    _test(MIRCOSEQSHN, 'samples', clear=True)
    


def test_pandas_datablock():
    pdbk = datablocks.DBX('datablocks.test.pandas.datablocks.PandasArray', f"pdbk")
    _test(pdbk)

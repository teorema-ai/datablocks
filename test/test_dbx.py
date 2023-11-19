import datablocks
import datablocks.dataspace


TESTLAKE = datablocks.dataspace.Dataspace.temporary()


def _test(dbx, topic=None, *, build=True, read=True, verbose=False):
    print()
    if verbose:
        print(f"intent: {dbx}\n", dbx.intent())
    if build:
        if verbose:
            print(f"extent: pre-build: {dbx}: \n", dbx.extent())
        dbx.build()
        if verbose:
            print(f"extent: post-build: {dbx}\n", dbx.extent())
    if read:
        if topic:
            if verbose:
                print(dbx.read(topic))
        else:
            if verbose:
                print(dbx.read())


MIRCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRCoHN', 'mircohn')\
        .Databuilder(dataspace=TESTLAKE)


MIRNA = datablocks.DBX('datablocks.test.micron.datablocks.miRNA', 'mirna').Datablock(verbose=True).SCOPE()


def test_mircohn():
    _test(MIRCOHN, 'logcounts')


def test_mirna():
    _test(MIRNA)


def test_mircoseq():
    MIR_COSEQS_NPASSES = 10
    MIR_COSEQS_SEQS_PER_RECORD = 300
    MIRCOSEQSHN = \
        datablocks.DBX('datablocks.test.micron.datablocks.miRCoSeqs', f"mircoseqshn_{MIR_COSEQS_NPASSES}_{MIR_COSEQS_SEQS_PER_RECORD}")\
            .Datablock(verbose=True).SCOPE(logcounts=MIRCOHN.READ('logcounts'), 
                                           logcontrols=MIRCOHN.READ('logcontrols'),
                                           seqs=MIRNA.READ(), 
                                           npasses=MIR_COSEQS_NPASSES, 
                                           nseqs_per_record=MIR_COSEQS_SEQS_PER_RECORD)
    _test(MIRCOSEQSHN, 'samples')


def test_pandas_datablock():
    pdbk = datablocks.DBX('datablocks.test.pandas.datablocks.PandasArray', f"pdbk")
    _test(pdbk)

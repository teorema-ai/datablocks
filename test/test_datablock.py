import datablocks
import datablocks.dataspace


TESTLAKE = datablocks.dataspace.Dataspace.temporary()


def test_micron():
    def _test(dbx, topic=None):
        print(f"intent: {dbx}\n", dbx.intent())
        print(f"extent: pre-build: {dbx}: \n", dbx.extent())
        dbx.build()
        print(f"extent: post-build: {dbx}\n", dbx.extent())
        if topic:
            print(dbx.read(topic))
        else:
            print(dbx.read())
    MIRCOHN = datablocks.DBX('datablocks.test.micron.datablocks.miRCoHN', 'mircohn')\
        .Databuilder(dataspace=TESTLAKE)
    _test(MIRCOHN, 'logcounts')

    MIRNA = datablocks.DBX('datablocks.test.micron.datablocks.miRNA', 'mirna').Datablock(verbose=True).SCOPE()
    _test(MIRNA)

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


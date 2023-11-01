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
    MIRCOHN = datablocks.DBX('datablocks.test.micron.datasets.miRCoHN', 'mircohn')\
        .Databuilder(dataspace=TESTLAKE)
    _test(MIRCOHN, 'counts')

    MIRCOSHN = datablocks.DBX('datablocks.test.micron.datasets.miRCoStats', 'mircoshn')\
        .Databuilder(dataspace=TESTLAKE)\
        .SCOPE(mirco=MIRCOHN.READ('counts'))
    _test(MIRCOSHN)


# SUMMARY 
datablocks is a Python package that manages datasets built out of Datablocks and residing in Dataspaces.
It is an experiment dataset management toolkit.

`datablocks.dbx.DBX` manages lifecycle of a dataset encapsulated in a `Datablock` class. 
* `Datablock` interface at a minimum should implement the following interface:
```
    class Datablock:
        TOPICS: dict # {topic -> root} 
        |
        FILENAME: str #TODO: -> PATHNAME
        
        @dataclass
        def SCOPE:
            ...

        def __init__(self,
            roots: dict{topic:str -> root:str|list[str]} | str | list[str],
            filesystem: fsspec.AbstractFileSystem,
            scope: Optional[SCOPE],
            *,
            debug=False,
            verbose=False,
        )
            ...

        def build(self):
            ...

        def read(self, topic: Optional[str]):
            ...

        [
        def valid(topic: Optional[str]):
            ...
        def metric(topic: Optional[str]) -> Float:
            ...
        ]

```
For convenience, `Datablock` can be made to be descendant of the abstract `datablocks.dbx.Datablock` class.  

# USAGE
* Installation
`pip install -e $DATABLOCKS`

* DBX definition:
`datablocks.dbx.DBX(datablock: Datablock, alias: str, verbose: bool, debug: bool, **databuilder_kwargs)` 
    or 
 `datablocks.dbx.DBX(path_to_datablock:str, alias:str, alias: str, verbose: bool, debug: bool, **databuilder_kwargs)`
can be used to 
    - build and cache a dataset
    - interrogate its build history, 
    - debug logging failures
    - read the data

* DBX arguments:
If `**scope` includes the result of other `DBX` as input specified as follows
```
    dbarray10 = datablocks.dbx.DBX(datablocks.test.datasets.PandasArray, 'dbarray10')
    datablocks.dbx.DBX(datablocks.test.datasets.PandasMultiplier, 'dbmult10_3')\
        .SCOPE(input=dbarray10.READ(topic), multiplier=3.0)
```
then `DBX` builds a lazy execution graph, whose nodes include (1) reading `dbarray10`, (2) building `dbmult10_3` from the inputs.
The graph is evaluated in an evaluation pool, potentially remotely, potentially in parallel, depending on the pool type.

* DBX path arguments
    # TODO: Use DBX.Transcribe docstr after testing it
    #...


# BASIC EXAMPLES
## BASH
* See available datablocks
```
    dbx "datablocks.DBX.show_datablocks()"
```
* See a datablock build record history
`dbx "help(datablocks.DBX.show_build_records)"`
without an alias -- all records for this `Datablock` class
```
    dbx "DBX('datablocks.test.pandas.datablocks.PandasArray').show_build_records()"
```
or with an alias -- records specific to this instance of the `Datablock` class
```
    DBX('datablocks.test.pandas.datablocks.PandasArray', 'dbk').show_build_records()
```

* Build a datablock
`dbx "help(datablocks.DBX.build)"`
```
>dbx "DBX('datablocks.test.pandas.datablocks.PandasArray', 'dbk')\
    .Datablock(verbose=True, build_delay_secs=10, echo_delay_secs=1)\
    .SCOPE(size=100).build()"
```

* Check result
```
    export DBK="DBX('datablocks.test.pandas.datablocks.PandasArray', 'dbk') # only declaration and alias matter; scope, etc. are retrieved from build records using alias
    >dbx "$DBK.show_build_records()"
    >dbx "$DBK.show_build_graph().status"
    >dbx "$DBK.show_build_graph().exception"
    >dbx "$DBK.show_build_graph().traceback"
    >dbx "$DBK.show_build_graph().result"
    >dbx "$DBK.show_build_graph().log()"
    >dbx "$DBK.show_build_batch_count()"
    >dbx "$DBK.show_build_batch_graph()"
```
* Read a datablock
`dbx "help(datablocks.DBX.read)"`
```
>export DBK="DBX('datablocks.test.pandas.datablocks.PandasArray', 'dbk')"
>dbx "$DBK.topics"
>dbx "$DBK.scope"
>dbx "$DBK.extent"
>dbx "$DBK.intent"
>dbx "$DBK.valid"
>dbx "$DBK.metric"
>dbx "$DBK.shortfall"
>dbx "$DBK.read()"
```

# DEBUGGING
```
    export PDA="DBX('datablocks.test.pandas.datablocks.PandasArray', 'pda')" 
    dbx "$PDA.build()"

    * block build records' tail: [returns a dataframe that can be subqueries]
    dbx "$PDA.show_build_records(tail=7)"

    * last block build record:
    dbx "$PDA.show_build_record()"

    * specified block build record with $ID
    dbx "$PDA.show_build_record(record=$ID)"
    
    * last record's graph:
        See the overall state, take note of exceptions and logs
    dbx "$PDA.show_build_graph()" 
    
    * specified record's graph
    dbx "$PDA.show_block_graph(record=$ID)" # use record index from output of `show_block_records`
    # Most of the `show_*` methods below take a record index and default to the last record.

    * graph transcript (detailed):
    dbx "$PDA.show_build_graph().transcript" # See the overall state, take note of exceptions and logs
    
    * batch count:
    dbx "print($PDA.show_build_batch_count())"

    * batch graph of batch 0:
    dbx "$PDA.show_build_batch_graph()"
    # or
    dbx "$PDA.show_build_batch_graph(batch=0)"

    * batch log
    #TODO: generate a test example with a non-empty log
    dbx "$PDA.show_build_batch_graph().log()"

    * batch exception
    #TODO: generate a test example with a non-None exception
    dbx "$PDA.show_build_batch_graph().exception"

    * batch traceback: 
    #TODO: generate a test example with a non-None traceback
        only if necessary -- can be very voluminous, 
        since traceback is replicated up the call tree:
    dbx "$PDA.show_build_batch_graph().traceback"

    * graph arg node:
        Use the output of `show_block_graph` to determine list of node indices:
      
```

# TRANSCRIBE
#... #TODO

# TEST
```
    pytest
```

# ENV
* DATABLOCKS_DATALAKE
or
* DATALAKE
default
* DATALAKE=$HOME/.cache/datalake


# FULL FEATURED EXAMPLES: MICRON
## BASH
* `datablocks
```
#> Define
export DATALAKE=$HOME/.cache/testlake
rm -rf $DATELAKE
export MIRCOHN="datablocks.DBX('datablocks_test.micron.micron_dbk.miRCoHN', verbose=True)"
export MIRCOS="datablocks.DBX('datablocks_test.micron.micron_dbk.miRCoStats').SCOPE(mirco=$MIRCOHN.READ('counts'))"
#> Examine
echo "$MIRCOS"
dbx "$MIRCOS"
dbx "help(datablocks_test.micron_dbk.miRCoHN)"
dbx "help(datablocks_test.micron_dbk.miRCoHN.SCOPE)"

dbx "$MIRCOS.SCOPE"
dbx "$MIRCOS.intent()"
dbx "$MIRCOS.extent()"
#> Build
#... Start with miRCoHN: upstream dependency
...

```

# DESIGN
* Futures throw contained exceptions upon `result()`, Responses do not [#TODO: should they?]

#datablocks
> SUMMARY 
datablocks is a Python package that manages datasets built out of Datablocks and residing in Dataspaces.
It is an experiment dataset management toolkit.

`datablocks.datablock.DBX` manages lifestages of a dataset encapsulated in a `Datablock` class.  
* `Datablock` interface at a minimum should implement the following interface:
```
    class Datablock:
        [TOPICS: dict] # {topic -> root}

        @dataclass
        def SCOPE:
            ...

        def __init__(self,
                    roots: dict{topic:str -> root:str|list[str]} | str | list[str],
                    filesystem: fsspec.AbstractFileSystem,
                    scope: Optional[SCOPE])

        def build(self):
            ...

        def read(self, topic: Optional[str]):
            ...
```
* Optionally, it is recommended that the following additional members methods be implemented.
    - `REVISION` [member]
    - `valid(topic: Optional[str])` 
    - `metric(topic: Optional[str]) -> Float`` 
*`datablocks.dbx.DBX(Datablock, 'alias', verbose: bool, debug: bool, **databuilder_kwargs)` 
    or 
 `datablocks.datablock.DBX('path.to.Datablock', 'alias', ...)`
can be used to 
* build and cache a dataset
* interrogate its build history, 
* debug logging failures
* read the data

If `**scope` includes the result of other `DBX` as input specified as follows
```
    dbarray10 = DB(datablocks.test.datasets.PandasArray, 'dbarray10')
    DB(datablocks.test.datasets.PandasMultiplier, 'dbmult10_3').SCOPE(input=dbarray10.READ(topic), multiplier=3.0)
```
then `DBX` builds a lazy execution graph, whose nodes include (1) reading `dbarray10`, (2) building `dbmult10_3` from the inputs.
The graph is evaluated in an evaluation pool, potentially remotely, potentially in parallel, depending on the pool type.

More details below.

#USAGE
## Basic
* Build a dataset 
```
>dbx "DBX('datablocks.test.datasets.PandasDataset', 'pd100', verbose=True, build_delay_secs=10).SCOPE(size=100).build()"
```
* Read a dataset
```
>dbx "DBX('datablocks.test.datablocks.PandasDatablock', 'pd100').read()"
```

#DEBUGGING
```
    export PDA="DBX('datablocks.test.datasets.PandasArray', 'pda')" 
    dbx "$PDA.build()"

    * all block build records:
    dbx "$PDA.show_build_records()"
    
    * last record's graph:
    dbx "$PDA.show_build_graph()" # See the overall state, take note of exceptions and logs
    
    * specified record's graph
    dbx "$PDA.show_block_graph(record=3)" # use record index from output of `show_block_records`
    # Most of the `show_*` methods below take a record index and default to the last record.
    
    * batch count:
    dbx "$PDA.show_block_count()"

    * batch graph of batch 0:
    dbx "$PDA.show_block_batch_graph()"
    # or
    dbx "$PDA.show_block_batch_graph(batch=0)"

    # Add exception tracebacks to print, but only if necessary -- can be very voluminous, 
    #  since traceback is replicated up the call tree:
    datablocks.exec "$PDA.show_block_batch_graph(_print='traceback')"
    #
    # See top block log or logpath(usually rather uninformative)
    datablocks.exec "$PDA.show_block_graph().log()"
    # or `logpath`, `logname`, `logspace`
    # See a batch log (usually there is only one):
    datablocks.exec "$PDA.show_block_batch_graph().log()"
    #
    # Also potentially useful:
    # Use the output of `show_block_graph` to determine list of node indices:
    datablocks.exec "$PDA.show_block_batch_graph(node=[]'1'], _print='traceback')"   
```
or in Python
```
    from datablocks.dbx import DBX
    import datablocks.test.datasets

    PDA=DBX(datablocks.test.datasets.PandasArray, 'pda')
    # or
    PDA=DBX("datablocks.test.datasets.PandasArray", 'pda')
    PDA.build()
    # etc
```

# TEST
```
    pytest
```

# EXAMPLES
## MICRON
```
# Define
export MIRCOHN="datablocks.DBX('datablocks.test.micron.datasets.miRCoHN')"
export MIRCOS="datablocks.DBX('datablocks.test.micron.datasets.miRCoStats').SCOPE(mirco=$MIRCOHN.data('counts'))"
# Examine
dbx.echo "$MIRCOS"
dbx "$MIRCOS"
dbx "$MIRCOS.SCOPE"
dbx "$MIRCOS.intent()"
dbx "$MIRCOS.extent()"
# Build
...

```

# DESIGN
* Futures throw contained exceptions upon `result()`, Responses do not [#TODO: should they?]

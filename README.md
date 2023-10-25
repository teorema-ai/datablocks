# datablocks
> SUMMARY 
datablocks is a Python package that manages datasets built out of Datablocks and residing in Dataspaces.
It is an experiment dataset management toolkit.

`datablocks.datablock.DBX` manages lifestages of a dataset encapsulated in a `Datablock` class.  
* `Datablock` interface at a minimum should implement the following interface:
```
    class Datablock:
        @dataclass
        def SCOPE:
            ...
        
        def build(scope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots):
            ...
        def read(scope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots):
            ...
```
* Optionally, it is recommended that the following additional members methods be implemented.
    - `VERSION` [member]
    - `valid(scope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots)` 
        . [`scope` can be ignored here if the validation can be done solely based on `roots` and `filesystem`]
    - `metric(scope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots) -> Float 
        . [`scope` can be ignored here if the metric can be computed solely based on `root` and `filesystem`]
*`datablocks.datablock.DBX(Datablock, 'alias')` or `datablocks.datablock.DBX('path.to.Datablock', 'alias')`
can be used to 
* build and cache a dataset
* interrogate its build history, 
* debug logging failures
* read the data

If `**scope` includes the result of other `DBX` as input specified as follows
```
    dbarray10 = DB(datablocks.test.datasets.PandasArray, 'dbarray10')
    DB(datablocks.test.datasets.PandasMultiplier, 'dbmult10_3').build(input=dbarray10.reader(), multiplier=3.0)
```
then `DBX` builds a lazy execution graph, whose nodes include (1) reading `dbarray10`, (2) building `dbmult10_3` from the inputs.
The graph is evaluated in an evaluation pool, potentially remotely, potentially in parallel, depending on the pool type.

More details below.

# USAGE
## Basic
* Build a dataset 
```
>datablocks.exec "DBX('datablocks.test.datasets.PandasDataset').DATABLOCK(verbose=True, build_delay_secs=10).SCOPE(size=100)"
```
* Read a dataset
```
>datablocks.exec "DBX('datablocks.test.datasets.PandasDataset').SCOPE(size=100).read()"
```


* See `datablocks.test.datasets.[PandasArray, PandasMultiplier]` for implementation details.
* A datablock implementation must implement 
    _ `init(root: str, filesystem: fsspec: AbstractFileSystem)
    - `build(**scope)`
    - `read(**scope)` [`scope` can be ignored here if the reading can be done solely from `root` and `filesystem`]


## Debugging
```
    export PDA="DBX('datablocks.test.datasets.PandasArray', 'pda')" 
    datablocks.exec "$PDA.build()"
    #
    # Show all block build records:
    datablocks.exec "$PDA.show_build_records"
    # See the last record's graph:
    datablocks.exec "$PDA.show_build_graph()" # See the overall state, take note of exceptions and logs
    # or
    datablocks.exec "$PDA.show_block_graph(record=3)" # use record index from output of `show_block_records`
    # Most of the `show_*` methods below take a record index and default to the last record.
    #
    # Check the number of batches:
    datablocks.exec "$PDA.show_block_nbatches()"
    # Look at the subgraph at batch 0:
    datablocks.exec "$PDA.show_block_batch_graph()"
    # or
    datablocks.exec "$PDA.show_block_batch_graph(batch=0)"
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
    import datablocks.test.datasets
    PDA=DB(datablocks.test.datasets.PandasArray, 'pda')
    # or
    PDA=DB("datablocks.test.datasets.PandasArray", 'pda')
    PDA.build()
    # etc
```
# TEST
```
    python $HOME/datablocks/datablocks/test/datasets.py [{TEST_NAME}]
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
dbx "$MIRCOS.scope"
dbx "$MIRCOS.intent()"
dbx "$MIRCOS.extent()"
# Build


```

# DESIGN
* Futures throw contained exceptions upon `result()`, Responses do not [#TODO: should they?]

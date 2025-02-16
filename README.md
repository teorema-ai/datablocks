# SUMMARY 
datablocks is a Python package that manages datasets built out of Datablocks and residing in Dataspaces.
It is an experiment dataset management toolkit.

`datablocks.dbx.DBX` manages lifecycle of a dataset encapsulated in a `Datablock` class. 
* `Datablock` interface at a minimum should implement the following interface:
```
    class Datablock:
        TOPICS: dict # {topic -> root} 
        |
        PATHNAME: str
        
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
            self.roots = roots
            self.filesystem = filesystem
            self.scope = scope
            if self.scope is None:
                self.scope = self.SCOPE()
            self.debug = False
            self.verbose = False

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

# DESIGN
* We build blocks and read blocks, which under the hood are examined one shard at a time,
  . and the missing shards are assembled into subblocks (batches), built one batch at a time,
  . and written one shard at a time.
  . Shards are necessary to resume partial builds or combine overlapping blocks.
  . Distinction between block/batch/shard appears only when SCOPE variables are of type BATCH, which means, they encapsulate multiple shards in one serial build.
* Futures throw contained exceptions upon `result()`, Responses do not [#TODO: should they?]

* Request -> evaluate -> Response [[-> Report -> Transcript] ... [-> Report -> Transcript]]
    . Request used for static graph definition
    . Task used for dynamic pool functionality implementation
	. `Request.evaluate(self) -> Response(request) { 
	    self._args_responses, self._kwargs_responses = \
            Request.evaluate_args_kwargs(request.args, request.kwargs)
      }
    . Report is an instantaneous stateless snapshot of Response.  Different reports taken 
      at different stages of the Request/Response lifecycle will be different and remain constant.
    `
    . `pool.evaluate(request)`  will send the whole subtree under `request` to pool and evaluate it there
        . some subtrees may live on different pools, so their evaluation will trigger transfers of requests to those pools
        . recall that `eval` infrastructure is designed to build coarse-grained (permanent) storage shards
            . so result communication should go through storage, not through pool Futures

	. `Response.result()`:
	```
        self.compute() {
            task(*(self._args_responses->results), **(self._kwargs_responses->results))
        }:
        if self.exception() is not None and self.request.get('throw', False):            
            raise self.exception().with_traceback(self.traceback())
        else:
            return _result
	```
    . `DBX._build_block_request_lifecycle_callback_`:
    ```
        report = response.report()
        task_id = response.id
        report_transcript = report.transcript() 
        ...
        _record.update(dict(
                            task_id=str(task_id),
                            runtime_secs=f"{runtime_secs}",
                            status=report_transcript['status'],
                            success=report_transcript['success'],
                            logspace=repr(logspace),
                            logname=logname,
                            report_transcript=transcriptstr,
                    ))
        ...
        record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
    ```
* Pool: Request.func -> Task -> pool.Request
    . request.apply(pool) -> pool.Request(cookie,
                                          func=pool.Task(pool, request.func),
                                          *request.args,
                                          **request.kwargs,
                                         ).set(throw=pool.throw)
        . pool.Request is a thin wrapper around Request
            . holds a pool cookie
            . avoids double-wrapping func if isinstance(func, pool.Task)
            . adapts __repr__, __tag__, __rebind__, __redefine__
    . pool.Request.evaluate(request) -> self.pool.evaluate(request):
        ```
            delayed = pool._delay_request(request)
            future = self._submit_delayed(delayed)
            response = pool.Response(pool, 
                                     request, 
                                     future, 
                                     done_callback=self._execution_done_callback)
        ```
    . pool.Logging:
        ._delay_request(request) -> essentially a noop:
        ```
            _args, _kwargs = self._delay_request_args_kwargs(request) # almost a noop
            delayed = request.rebind(*_args, **_kwargs)
        ```
        ._submit_delayed(delayed):
        ```
            future = self.executor.submit(delayed)
        ```
    . pool.Dask:
        ._delay_request(request):
        ```
            # rewrite the request graph in Dask space
            # each node, however, may wrap an inner pool Task, which can manage logging, validation, etc. 
            _delayed_args, _delayed_kwargs = self._delay_request_args_kwargs(request)
            delayed = dask.delayed(request.task)(*_delayed_args, dask_key_name=request.dask_key, **_delayed_kwargs)
 
        ```
        ._submit_delayed(self, delayed):
        ```
            future = self.client.compute(delayed)
        ```

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
    dbx.print "datablocks.DBX.show_datablocks()"
```
* See a datablock build record history
`dbx "help(datablocks.DBX.show_build_records)"`
without an alias -- all records for this `Datablock` class
```
    dbx.print "DBX('datablocks.test.pandas.datablocks.PandasArray').show_records()"
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



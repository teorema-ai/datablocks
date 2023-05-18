# datablocks
Numerical experiment dataset management toolkit.

# Usage
## Basic
* Build a dataset 
```
>datablocks.exec "datablocks.datablock.DB('datablocks.test.datasets.PandasDataset', verbose=True, build_delay_secs=10).build(size=100)"
```
* Read a dataset
```
>datablocks.exec "datablocks.datablock.DB('datablocks.test.datasets.PandasDataset', verbose=True, build_delay_secs=10).read()"
```


* See `datablocks.test.datasets.PandasDataset` for implementation details.
* A datablock implementation must implement 
    - `build(root, filesystem, **scope)`
    - `read(root, filesystem, **scope)` [`scope` can be ignored here if the reading can be done solely from `root` and `filesystem`]
* Optionally, it is recommended that the following additional members methods be implemented.
    - `version` [member]
    - `valid(root, filesystem, **scope)` [`scope` can be ignored here if the validation can be done solely based on `root` and `filesystem`]
    - `metric(root, filesystem, **scope) -> Tuple[Union[Float, Int]]` [`scope` can be ignored here if the metric can be computed solely based on `root` and `filesystem`]

# PRINCIPLES
* Futures throw contained exceptions upon `result()`, Responses do not [#TODO: should they?]

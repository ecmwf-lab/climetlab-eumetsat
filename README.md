## eumetsat

A source plugin for climetlab
```python

!pip install climetlab climetlab-eumetsat
import climetlab as cml
ds = cml.load_source("eumetsat-datastore", collection_id="EO:EUM:DAT:METOP:GLB-SST-NC",)
ds.to_xarray()
```

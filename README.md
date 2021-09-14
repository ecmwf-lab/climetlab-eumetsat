## eumetsat

A dataset plugin for climetlab for the dataset eumetsat/mydataset.


Features
--------

In this README is a description of how to get the eumetsat.

## Datasets description

There are two datasets: 

### 1 : `mydataset`


### 2
TODO


## Using climetlab to access the data (supports grib, netcdf and zarr)

See the demo notebooks here (https://github.com/ecmwf-lab/climetlab-eumetsat/notebooks

https://github.com/ecmwf-lab/climetlab-eumetsat/notebooks/demo_mydataset.ipynb
[nbviewer] (https://nbviewer.jupyter.org/github/climetlab_eumetsat/blob/main/notebooks/demo_mydataset.ipynb) 
[colab] (https://colab.research.google.com/github/climetlab_eumetsat/blob/main/notebooks/demo_mydataset.ipynb) 

The climetlab python package allows easy access to the data with a few lines of code such as:
```

!pip install climetlab climetlab-eumetsat
import climetlab as cml
ds = cml.load_dataset(""eumetsat-mydataset", date='20201231',)
ds.to_xarray()
```

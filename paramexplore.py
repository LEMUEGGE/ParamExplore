import holoviews as hv
import pandas as pd
import numpy as np
from holoviews.operation.datashader import datashade,dynspread
import datashader as ds
from bokeh.io import curdoc
import dask as da
from sklearn.model_selection import ParameterGrid
from collections import OrderedDict
import itertools
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler,visualize


def param_grid_search(func, param_grid_dict):
    """Given func and a dictionary with position and iterable parameter states {1:list, 2:list},
     creates a dask function graph for a parallelized function evaluation 
    within the specified parameter grid"""
    param_dict = {id(obj):obj for obj in itertools.chain(*param_grid_dict.values())}
    param_grid_dict_id = {key : [id(obj) for obj in param_grid_dict[key]] for key in param_grid_dict.keys()} 
    grid = np.array(np.meshgrid(*[i[1] for i in sorted(param_grid_dict_id.items())])).reshape(len(param_grid_dict_id.keys()),-1)
    
    DAG = OrderedDict()
    for n,state in enumerate(grid.T):
        task = (func,)+tuple([param_dict[key] for key in state])
        DAG.update({'state_{}'+str(n):task})

    return DAG
    



def rolling_mean(df,window_size1,window_size2):
    df_ = df.copy()
    df_.y = df.y.rolling(window_size1).median()-df.y.rolling(window_size2).median()
    
    return df_
    
DAG = param_grid_search(rolling_mean,{1:[pd.DataFrame({'x':np.arange(1000000),'y':np.sin(np.arange(1000000))+np.random.rand(1000000)*0.1}),
                    pd.DataFrame({'x':np.arange(1000000),'y':np.sin(np.arange(1000000))+30})],2:[10,100,1000,40,20,10,20,4,5],3:[10,100,1000]})

def p(df_list):
    return
def plot_df(df_list):
    renderer = hv.renderer('bokeh').instance(mode='server')
    print(df_list)
    lines = {i : hv.Curve(df_list[i],kdims=['x'],vdims=['y']) for i in range(len(df_list))}
    linespread  = dynspread(datashade(hv.NdOverlay(lines,     kdims='k')), aggregator=ds.count_cat('k')).opts(**{'plot' : {'height':400, 'width':1400}})
    return renderer.get_plot(linespread).state
param_states = list(DAG.keys())
DAG.update({'plot':(p,list(param_states))})

#curdoc().add_root(da.get(DAG,'plot'))
with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof,CacheProfiler() as cprof:
    da.multiprocessing.get(DAG,'plot')
    #da.get(DAG,'plot')
curdoc().add_root(visualize([prof,rprof,cprof]))

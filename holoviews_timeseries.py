import holoviews as hv
import pandas as pd
import numpy as np
from holoviews.operation.datashader import datashade,dynspread
import datashader as ds
from bokeh.io import curdoc
import dask as da
from sklearn.model_selection import ParameterGrid
from collections import OrderedDict
"""
index = np.arange('2005-02', '2006-03', dtype='datetime64[m]')
df = pd.DataFrame({'data':np.random.rand(index.shape[0]),'data2':np.random.rand(index.shape[0])+5},index=index)
df['x'] = index
print(len(df))

obj = (datashade(hv.Curve(df,vdims=['data2'],kdims=['x'])) * datashade(hv.Curve(df,vdims=['data'],kdims=['x']))).opts(**{'plot' : {'height':400, 'width':1400}})

renderer = hv.renderer('bokeh').instance(mode='server')

document = renderer.get_plot(obj).state
curdoc().add_root(document)

"""

def param_grid_search(func, dataframe, pos_val_dict):
    """Given func and a dictionary {1:list, 2:list}, creates a dask function graph for a parallelized function evaluation 
    within the specified parameter grid"""
    
    grid = np.array(np.meshgrid(*[i[1] for i in sorted(pos_val_dict.items())])).reshape(len(pos_val_dict.keys()),-1)
    
    DAG = OrderedDict()
    for n,state in enumerate(grid.T):
        task = (func,dataframe)+tuple(state)
        DAG.update({'grid_n_'+str(state):task})
        #print(DAG)
        
    return DAG
    



def rolling_mean(df,window_size1,window_size2):
    df_ = df.copy()
    df_.y = df.y.rolling(window_size1).mean()-df.y.rolling(window_size2).mean()
    
    return df_
    
DAG = param_grid_search(rolling_mean,pd.DataFrame({'x':np.arange(10000),'y':np.sin(np.arange(10000))+np.random.rand(10000)*0.1}),{2:[10,100,1000],3:[10,100,1000]})

def plot_df(df_list):
    renderer = hv.renderer('bokeh').instance(mode='server')
    print(df_list)
    lines = {i : hv.Curve(df_list[i],kdims=['x'],vdims=['y']) for i in range(len(df_list))}
    linespread  = dynspread(datashade(hv.NdOverlay(lines,     kdims='k')), aggregator=ds.count_cat('k')).opts(**{'plot' : {'height':400, 'width':1400}})
    return renderer.get_plot(linespread).state
param_states = list(DAG.keys())
DAG.update({'plot':(plot_df,list(param_states))})

print(DAG)
curdoc().add_root(da.get(DAG,'plot'))

import tushare as ts

df = ts.get_tick_data('600848',date='2014-01-09')
df.head(10)
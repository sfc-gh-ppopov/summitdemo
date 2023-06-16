import pandas as pd
import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import logging


st.title('Power grids report')
st.text('In this report we will show you the promising areas for building our next')
st.text('electricity line.')
st.text("At first let\'s look at the following picture:")
map_style = eval(open("mapconfig.json").read())
sess = Session.builder.configs(st.secrets["snowflake"]).create()
sess.sql("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'").collect()
spatialfeatures = sess.table('grid_lte_coverage').select(col("grid"),col("lte_gaps"))
logging.info('CUSTOM LOG: SnowPark DF is there')
df = pd.DataFrame(spatialfeatures.collect())
logging.info('CUSTOM LOG: Pandas df is there')
map = KeplerGl(config = map_style)
map.add_data(data=df, name="data")

keplergl_static(map, height = 600, width = 800)
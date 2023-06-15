import pandas as pd
import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import logging



map_style = eval(open("mapconfig.json").read())
sess = Session.builder.configs(st.secrets["snowflake"]).create()
st.markdown("The color visualizes the average yearly precipitation (red: low precipitation, turquoise: high).", unsafe_allow_html=True)
st.markdown("The width of hexagons visualizes the population density. For more details click legend icon.", unsafe_allow_html=True)
spatialfeatures = sess.table('grid_lte_coverage').select(col("grid"),col("lte_gaps"))
logging.info('CUSTOM LOG: SnowPark DF is there')
df = pd.DataFrame(spatialfeatures.collect())
logging.info('CUSTOM LOG: Pandas df is there')
map = KeplerGl(config = map_style)
map.add_data(data=df, name="data")
keplergl_static(map, height = 600, width = 900, )
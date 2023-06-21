import pandas as pd
import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import logging


st.title('Power Grids Report')
st.subheader('Summary:')
st.text('In this report, we aim to identify the most promising areas for the construction of')
st.text('our upcoming electricity line. At first let\'s look at the following visualization')
st.text("which depicts areas without mobile network and the access to the electricity.")
map_style = eval(open("mapconfig.json").read())
sess = Session.builder.configs(st.secrets["snowflake"]).create()
sess.sql("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'").collect()
electricity_coverage = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("electricity_coverage"))
where_to_build = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("where_to_build"))
boundaries = sess.table('geolab.demotest.NL_ADMINISTRATIVE_AREAS_viz').select(col("geog"))
logging.info('CUSTOM LOG: SnowPark DF is there')
electricity_coverage = pd.DataFrame(electricity_coverage.collect())
where_to_build = pd.DataFrame(where_to_build.collect())
boundaries = pd.DataFrame(boundaries.collect())
logging.info('CUSTOM LOG: Pandas df is there')

map = KeplerGl(config = map_style)
map.add_data(data=electricity_coverage, name="electricity_coverage")
map.add_data(data=where_to_build, name="where_to_build")
map.add_data(data=boundaries, name="boundaries")

keplergl_static(map, height = 600, width = 800)
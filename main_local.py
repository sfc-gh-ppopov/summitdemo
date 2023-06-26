import pandas as pd
import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import logging


st.title('Power Grids Report')

map_style_1 = eval(open("./styles/mapconfig_1.json").read())
map_style_2 = eval(open("./styles/mapconfig_2.json").read())
map_style_3 = eval(open("./styles/mapconfig_3.json").read())
map_style_4 = eval(open("./styles/mapconfig_4.json").read())
map_style_5 = eval(open("./styles/mapconfig_5.json").read())
map_style_6 = eval(open("./styles/mapconfig_6.json").read())

#sess = Session.builder.configs(st.secrets["snowflake"]).create()
#sess.sql("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'").collect()

st.subheader('Summary')
st.write("""In this report, we aim to identify the most promising areas for the construction of
the new cell towers. Such areas need to satisfy two conditions:""")
st.markdown("1. A tower is needed there, because there is no 4G signal in that area")
st.markdown("2. A tower can be built there, because there is electricity line nearby""")

#=======================================
st.subheader("Dataset 1: Cell towers")
st.write("Load information about cell towers from OpenCellId dataset. For each tower we know location, range and type of network.")

df_1_1 = pd.read_csv('./datasets/dataset_1_1.csv')
data_1_1 = df_1_1[['LOCATION']]

df_1_2 = pd.read_csv('./datasets/dataset_1_2.csv')
data_1_2 = df_1_2[['LOCATION']]

map1 = KeplerGl(config = map_style_1)
map1.add_data(data=data_1_1, name="cell_towers")
map1.add_data(data=data_1_2, name="search_result")

keplergl_static(map1, height = 600)

#=======================================
st.subheader("Dataset 2: Electricity grid")
st.write("Data about power lines in 5 provinces in the Netherlands. Stored in local coordinate system, SRID=28876")

df_2 = pd.read_csv('./datasets/dataset_2.csv')
data_2 = df_2[['GEOG']]

map2 = KeplerGl(config = map_style_2)
map2.add_data(data=data_2, name="electricity_coverage")

keplergl_static(map2, height = 600)

#=======================================
st.subheader("Dataset 3: The Netherlands administrative boundaries")
st.write("Load data from a Shapefile using custom UDF. The coordinate system is UTM, SRID=32231")

df_3 = pd.read_csv('./datasets/dataset_3.csv')
data_3 = df_3[['GEOG']]

map3 = KeplerGl(config = map_style_3)
map3.add_data(data=data_3, name="boundaries")

keplergl_static(map3, height = 600)


#=======================================
st.subheader("Cell towers with coverage polygons")
st.write("Show polygons of each cell tower")

df_4_1 = pd.read_csv('./datasets/dataset_4_1.csv')
data_4_1 = df_4_1[['COVERAGE']]

map4_1 = KeplerGl(config = map_style_4)
map4_1.add_data(data=data_4_1, name="cell_towers")

keplergl_static(map4_1, height = 600)

#=======================================
st.subheader("Zones in proximity of power line")
st.write("Here we used ST_BUFFER to find all areas that are close to power line.")

df_4_2 = pd.read_csv('./datasets/dataset_4_2.csv')
data_4_2 = df_4_2[['ELECTRICITY_COVERAGE']]

map4_2 = KeplerGl(config = map_style_6)
map4_2.add_data(data=data_4_2, name="grid_data")

keplergl_static(map4_2, height = 600)

#======================================
st.subheader("Cell coverage per province")
st.write("Union all coverage polygons")

df_5 = pd.read_csv('./datasets/dataset_5.csv')
data_5_1 = df_5[['LTE_COVERAGE']]
data_5_2 = df_5[['WHERE_TO_BUILD']]

map5 = KeplerGl(config = map_style_5)
map5.add_data(data=data_5_1, name="lte_coverage")
map5.add_data(data=data_5_2, name="lte_gaps")

keplergl_static(map5, height = 600)

#=======================================
st.subheader("Areas where to build new towers")
st.write("Add power grid data to the picture")

df_6 = pd.read_csv('./datasets/dataset_6.csv')
data_6_1 = df_6[['ELECTRICITY_COVERAGE']]
data_6_2 = df_6[['WHERE_TO_BUILD']]

map6 = KeplerGl(config = map_style_6)
map6.add_data(data=data_6_1, name="grid_data")
map6.add_data(data=data_6_2, name="lte_gaps")

keplergl_static(map6, height = 600)
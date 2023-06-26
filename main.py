import pandas as pd
import streamlit as st
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


st.title('Power Grids Report')

map_style_1 = eval(open("./styles/mapconfig_1.json").read())
map_style_2 = eval(open("./styles/mapconfig_2.json").read())
map_style_3 = eval(open("./styles/mapconfig_3.json").read())
map_style_4 = eval(open("./styles/mapconfig_4.json").read())
map_style_5 = eval(open("./styles/mapconfig_5.json").read())
map_style_6 = eval(open("./styles/mapconfig_6.json").read())

sess = Session.builder.configs(st.secrets["snowflake"]).create()
sess.sql("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'").collect()

st.subheader('Summary:')
st.text('In this report, we aim to identify the most promising areas for the construction of')
st.text('our upcoming electricity line. At first let\'s look at the following visualization')
st.text("which depicts areas without mobile network and the access to the electricity.")

#=======================================
st.subheader("Dataset 1: Cell towers")
st.write("Load information about cell towers from OpenCellId dataset. For each tower we know location, and type of network.")
cell_towers = sess.table('geolab.demotest.cell_towers_with_coverage').select(col("location"))
cell_towers = pd.DataFrame(cell_towers.collect())
map1 = KeplerGl(config = map_style_1)
map1.add_data(data=cell_towers, name="cell_towers")

keplergl_static(map1, height = 600)

#=======================================
st.subheader("Dataset 2: Electricity grid")
st.write("Data about power lines in 5 provinces in the Netherlands. Stored in local coordinate system, SRID=28876")

cables = sess.table('geolab.demotest.nl_cables_stations_viz').select(col("geog"))
cables = pd.DataFrame(cables.collect())

map2 = KeplerGl(config = map_style_2)
map2.add_data(data=cables, name="electricity_coverage")

keplergl_static(map2, height = 600)

#=======================================
st.subheader("Dataset 3: The Netherlands administrative boundaries")
st.write("Load data from a Shapefile using custom UDF. The coordinate system is UTM, SRID=32231")
boundaries = sess.table('geolab.demotest.NL_ADMINISTRATIVE_AREAS_viz').select(col("geog"))
boundaries = pd.DataFrame(boundaries.collect())

map3 = KeplerGl(config = map_style_3)
map3.add_data(data=boundaries, name="boundaries")

keplergl_static(map3, height = 600)


#=======================================
st.subheader("Cell towers with coverage polygons")
st.write("Show polygons of each cell tower")

cell_towers = sess.table('geolab.demotest.cell_towers_with_coverage').select(col("coverage"))
cell_towers = pd.DataFrame(cell_towers.collect())
map4 = KeplerGl(config = map_style_4)
map4.add_data(data=cell_towers, name="cell_towers")

keplergl_static(map4, height = 600)

#=======================================
st.subheader("Cell coverage per province")
st.write("Union all coverage polygons")

lte_covered = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("lte_coverage"))
lte_gaps = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("where_to_build"))

map5 = KeplerGl(config = map_style_5)
map5.add_data(data=pd.DataFrame(lte_covered.collect()), name="lte_coverage")
map5.add_data(data=pd.DataFrame(lte_gaps.collect()), name="lte_gaps")

keplergl_static(map5, height = 600)

#=======================================
st.subheader("Areas where to build new towers")
st.write("Add power grid data to the picture")

grid_data = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("electricity_coverage_cut"))
lte_gaps = sess.table('geolab.demotest.grid_lte_coverage_viz').select(col("where_to_build"))

map6 = KeplerGl(config = map_style_6)
map6.add_data(data=pd.DataFrame(grid_data.collect()), name="grid_data")
map6.add_data(data=pd.DataFrame(lte_gaps.collect()), name="lte_gaps")

keplergl_static(map6, height = 600)

sess.close()
import pandas as pd
import streamlit as st
import os
import datetime
from datetime import datetime, time
import plotly.express as px
import plotly.graph_objects as go
#from st_aggrid import AgGrid, GridOptionsBuilder
from streamlit_extras.colored_header import colored_header
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine, text
from streamlit_extras.tags import tagger_component
import random
import numpy as np
import dataframe_image as dfi
import pygwalker as pyg
import streamlit.components.v1 as components

pd.options.mode.chained_assignment = None  # default='warn'


st.set_page_config(page_title = "Home Page", layout="wide")

# st.markdown(
#     """
#     <style>
#         footer {display: none}
#         [data-testid="stHeader"] {display: none}
#     </style>
#     """, unsafe_allow_html = True
# )

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html = True)

colored_header(
        label="Nokia Hourly Filter Dashboard",
        description="",
        color_name="red-70",
)
st.markdown('<div class="seperator"></div>', unsafe_allow_html = True)
identifier= 'remote_datbase_secrets'

db_name=st.secrets[identifier]["db_name"]
filter_table=st.secrets[identifier]["filter_table"]
database_table=st.secrets[identifier]["database_table"]
host= st.secrets[identifier]["host"]
username= st.secrets[identifier]["username"]
password=st.secrets[identifier]["password"]
port=st.secrets[identifier]["port"]
db_type=st.secrets[identifier]["type"]

#engine = create_engine(f"{db_type}://{username}:{password}@{host}:{port}/{db_name}?charset=utf8mb4")

engine = create_engine(f"{db_type}://{username}:{password}@{host}:{port}/{db_name}")
colors= ['lightblue', 'orange', 'bluegreen', 'blue', 'violet', 'red', 'green', 'yellow']*10
random_color = random.choice(colors)
violet_color= [random_color,]*60
all_cells=23025
nokia_cells=9249

#======= Read database data and create dataframes ========#
@st.cache_data
def get_data(columns, table):
    return pd.read_sql(f"SELECT {columns} FROM {table}", engine)

# if st.toggle('Show Tables'):
#     tables=pd.read_sql('SELECT * FROM pg_catalog.pg_tables;', engine)
#     tables

filter_columns='"SiteId", "SiteName", "AlarmName", "FormattedDatetime", "AlarmInfo", "FilterIdentifier", "FilterDate"'
database_columns ='"SiteId", "Office", "2G_Cells", "3G_Cells", "4G_Cells", "Cascaded"'
filters = get_data(filter_columns,filter_table)
sites_database = get_data(database_columns, database_table)

#======= Merge other needed data ========#
n_df = pd.merge(filters, sites_database, on ='SiteId', how ='left')

#======= Create Site Filter Identifier ========#

n_df['SiteFilterIdentifier'] = n_df[['SiteId', 'FilterIdentifier']].agg('-'.join, axis=1)

#======= Filter by Date ========#


col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

with col6:
    st.markdown('<p class="date-select">Select Date</p>', unsafe_allow_html = True)

with col7:
    selected_date = st.selectbox('', options=n_df['FilterDate'].unique(), index= len(n_df['FilterDate'].unique())-1, label_visibility='collapsed')
    n_df = n_df.query(
    "FilterDate == @selected_date"
    )


#======= Dataframes ========#

n_df_down_2G= n_df[["SiteId","FilterIdentifier", "SiteFilterIdentifier", "AlarmName"]].query('AlarmName == "BTS O&M LINK FAILURE"').drop_duplicates(['SiteFilterIdentifier'])
n_df_down_3G= n_df[["SiteId","FilterIdentifier", "SiteFilterIdentifier", "AlarmName"]].query('AlarmName == "WCDMA BASE STATION OUT OF USE"').drop_duplicates(['SiteFilterIdentifier'])
n_df_down_4G= n_df[["SiteId","FilterIdentifier", "SiteFilterIdentifier", "AlarmName"]].query('AlarmName == "NE3SWS AGENT NOT RESPONDING TO REQUESTS"').drop_duplicates(['SiteFilterIdentifier'])
n_df_down_all= n_df.query('AlarmName == "BTS O&M LINK FAILURE" or AlarmName == "WCDMA BASE STATION OUT OF USE" or AlarmName == "NE3SWS AGENT NOT RESPONDING TO REQUESTS"')
down_cells_count = len(n_df[(n_df["AlarmName"]=="BCCH MISSING") | (n_df["AlarmName"]=="WCDMA CELL OUT OF USE")])


@st.cache_data
def add_cells_count(df):
    df['F_2G_Cells'] = df.groupby('SiteFilterIdentifier')['AlarmName'].transform(lambda x: x[x == 'BCCH MISSING'].count())
    df['F_3G_Cells'] = df.groupby('SiteFilterIdentifier')['AlarmName'].transform(lambda x: x[x == 'WCDMA CELL OUT OF USE'].count())
    return df

n_df=add_cells_count(n_df)


@st.cache_data
def v_lookup_data(df1, df2, df3):
    df1 = pd.merge(df1, df2[['SiteFilterIdentifier','AlarmName']], on ='SiteFilterIdentifier', how ='left')
    df1.rename(columns = {"AlarmName_y": "Down2G"}, inplace = True)
    df1 = pd.merge(df1, df3[['SiteFilterIdentifier','AlarmName']], on ='SiteFilterIdentifier', how ='left')
    df1.rename(columns = {"AlarmName_x": "AlarmName", "AlarmName": "Down3G"}, inplace = True)
    return df1

n_df = v_lookup_data(n_df, n_df_down_2G, n_df_down_3G)
n_df['final_2G'] = np.where(n_df['Down2G']=='BTS O&M LINK FAILURE', n_df['2G_Cells'], n_df['F_2G_Cells'])
n_df['final_3G'] = np.where(n_df['Down3G']=='WCDMA BASE STATION OUT OF USE', n_df['3G_Cells'], n_df['F_3G_Cells'])


final_down_cells=n_df[['SiteId', 'FilterIdentifier', 'final_2G', 'final_3G']].drop_duplicates()
final_down_cells['final_total_down_cells']=final_down_cells['final_2G']+final_down_cells['final_3G']
final_2G_down_cells = final_down_cells['final_2G'].sum()
final_3G_down_cells = final_down_cells['final_3G'].sum()
final_total_down_cells = final_down_cells['final_total_down_cells'].sum()
down_cells_per_filter = final_down_cells.groupby('FilterIdentifier', as_index = False)['final_total_down_cells'].sum()

final_2G_down_sites=n_df_down_2G['SiteId'].nunique()
final_3G_down_sites=n_df_down_3G['SiteId'].nunique()
final_total_down_sites=final_2G_down_sites+final_3G_down_sites


expected_ava= 100-(((final_total_down_cells)*100)/(nokia_cells*48))

#======= Main Page ========#

title_col, emp_col, last_filter_col, down_cells_col, down_sites_col, expected_ava_col = st.columns([1,0.2,1,1,1,1])


with title_col:
    st.markdown('<p class="dashboard_title">Hourly<br>Dashboard</p>', unsafe_allow_html = True)

with last_filter_col:
    with st.container():
        st.markdown(f'<p class="cont_text">Recent Filter<br></p><p class="cont_details">{n_df["FilterIdentifier"].unique().max()[15:20]}</p>', unsafe_allow_html = True)
with down_cells_col:
    with st.container():
        st.markdown(f'<p class="cont_text">Sum of Down Cells<br></p><p class="cont_details">{int(final_total_down_cells)}</p>', unsafe_allow_html = True)

with down_sites_col:
    with st.container():
        st.markdown(f'<p class="cont_text">Unique Down Sites<br></p><p class="cont_details">{int(final_total_down_sites)}</p>', unsafe_allow_html = True)

with expected_ava_col:
    with st.container():
        st.markdown(f'<p class="cont_text">Expected Availability<br></p><p class="cont_details">{round(expected_ava, 2)}%</p>', unsafe_allow_html = True)







n_df_down_pivot= pd.pivot_table(n_df_down_all, values='SiteName', index='FilterIdentifier', columns='AlarmName', aggfunc='count')
n_df_down_pivot = n_df_down_pivot.reset_index(drop = False)
n_df_down_pivot.rename(columns = {'BTS O&M LINK FAILURE':'2G', 'WCDMA BASE STATION OUT OF USE':'3G','NE3SWS AGENT NOT RESPONDING TO REQUESTS':'4G'}, inplace = True)
n_df_down_pivot["Time"]= n_df_down_pivot.FilterIdentifier.str[15:20]

n_df_down_pivot= n_df_down_pivot[["Time","2G","3G","4G"]]
n_df_down_pivot["2G"] = np.where(n_df_down_pivot["2G"].isnull(), 0, n_df_down_pivot["2G"])
n_df_down_pivot["3G"] = np.where(n_df_down_pivot["3G"].isnull(), 0, n_df_down_pivot["3G"])
down_cells_per_filter['Time']=down_cells_per_filter.FilterIdentifier.str[15:20]

st.markdown('<div class="seperator"></div>', unsafe_allow_html = True)

#======= Hourly Down Sites Graph ========#
plot = go.Figure(data=[go.Scatter(
    name ='2G Down Sites',
    x = n_df_down_pivot.Time,
    y = n_df_down_pivot['2G'],
    text=n_df_down_pivot['2G'],
    showlegend=False,
    mode = "lines+markers+text",
    textposition='top center',
   ),
])
plot.update_layout(
    title={
        'text': "Down Sites vs Time",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
    )
plot.update_traces(line=dict(width=4))
st.plotly_chart(plot, use_container_width=True)


#======= Hourly Down Cells Graph ========#
plot = go.Figure(data=[go.Scatter(
    name ='Down Cells',
    x = down_cells_per_filter.Time,
    y = down_cells_per_filter['final_total_down_cells'],
    text=down_cells_per_filter['final_total_down_cells'],
    showlegend=False,
    mode = "lines+markers+text",
    textposition='top center', 
   ),
])
plot.update_layout(
    title={
        'text': "Down Cells vs Time",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
    )
plot.update_traces(line=dict(width=4))
st.plotly_chart(plot, use_container_width=True)



 ################################################################################################################################



st.write('Selected Filter Analysis')

selected_filter = st.selectbox('Select Filter', options=n_df['FilterIdentifier'].unique(), index= len(n_df['FilterIdentifier'].unique())-1)
n_df_filtered = n_df.query(
    "FilterIdentifier == @selected_filter"
)

n_df_filtered['TimeReference']= pd.to_datetime(n_df_filtered['FilterIdentifier'].str[4:14]+' '+ n_df_filtered['FilterIdentifier'].str[15:], format='%Y-%m-%d %H:%M:%S')
n_df_filtered['DownHours'] = (n_df_filtered.TimeReference - n_df.FormattedDatetime) / pd.Timedelta(hours=1)
n_df_filtered['totalCells']= n_df_filtered['final_2G']+n_df_filtered['final_3G']

n_df_down_filtered= n_df_filtered.query('AlarmName == "BTS O&M LINK FAILURE" or AlarmName == "WCDMA BASE STATION OUT OF USE"')


#---Final Down Sites DataFrams---#
down_sites = n_df_down_filtered[['SiteId','Office', 'SiteName', 'AlarmName', 'FormattedDatetime', 'final_2G', 'final_3G', 'totalCells', 'DownHours']]
down_sites.rename(columns = {"SiteId": "Site ID", "FormattedDatetime": "Alarm Time", "final_2G": "2G Cells",  "final_3G": "3G Cells",  "totalCells": "Total Cells", "DownHours": "Aging"}, inplace = True)
down_sites = down_sites.reset_index(drop=True).set_index('Site ID').sort_values(by='Alarm Time', ascending=True)
#---End of Final Down Sites DataFrams---#


#--- Down Sites Pivot And Graph---#
n_df_down_sites_pivot= pd.pivot_table(n_df_down_filtered, values='SiteName', index='AlarmName', columns='Office', aggfunc='count')
n_df_down_sites_pivot = n_df_down_sites_pivot.reset_index(drop = False)

plot = go.Figure(data=[go.Bar(
    name ='Banisuef',
    x = n_df_down_sites_pivot.AlarmName,
    y = n_df_down_sites_pivot.Banisuief,
    text=n_df_down_sites_pivot.Banisuief,
    marker_color='#2553d2'
   ),
    go.Bar(
    name ='Fayoum',
    x = n_df_down_sites_pivot.AlarmName,
    y = n_df_down_sites_pivot.Fayoum,
    text=n_df_down_sites_pivot.Fayoum,
    marker_color='#e61d61'
   )
])
plot.update_layout(
    title={
        'text': "Down Sites Per Office",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
    )
plot.update_yaxes(showticklabels=False)
st.plotly_chart(plot, use_container_width=True)



n_df_down_sites_vs_cells_pivot= pd.pivot_table(n_df_down_filtered, values=['final_2G', 'final_3G', 'totalCells'], index='SiteId', aggfunc='max')
n_df_down_sites_vs_cells_pivot = n_df_down_sites_vs_cells_pivot.reset_index(drop = False)
n_df_down_sites_vs_cells_pivot.rename(columns = {"final_2G": "2G", "final_3G": "3G"}, inplace = True)
n_df_down_sites_vs_cells_pivot=n_df_down_sites_vs_cells_pivot.sort_values(by='totalCells', ascending=False)
final_down_cells_without_zero = final_down_cells.drop(final_down_cells[(final_down_cells['final_total_down_cells'] == 0) | (final_down_cells['final_total_down_cells'].isnull())].index)
final_down_cells_without_zero = final_down_cells_without_zero.query(
    "FilterIdentifier == @selected_filter"
)
final_down_cells_without_zero = final_down_cells_without_zero.sort_values(by='final_total_down_cells', ascending=False)
plot = go.Figure(data=[go.Bar(
    name ='2G Cells',
    x = n_df_down_sites_vs_cells_pivot.SiteId,
    y = n_df_down_sites_vs_cells_pivot['2G'],
    text=n_df_down_sites_vs_cells_pivot['2G'],
    marker_color='#f2b5d4'
   ), go.Bar(
    name ='3G Cells',
    x = n_df_down_sites_vs_cells_pivot.SiteId,
    y = n_df_down_sites_vs_cells_pivot['3G'],
    text=n_df_down_sites_vs_cells_pivot['3G'],
    marker_color='#b2f7ef'
   ),
])
plot.update_layout(
    title={
        'text': "Down Sites with Cells Count",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})
plot.update_xaxes(tickangle = 90, title_font = {"size": 20},)
plot.update_yaxes(showticklabels=False)
st.plotly_chart(plot, use_container_width=True)


n_df_down_sites_vs_aging_pivot= pd.pivot_table(n_df_down_filtered, values=['DownHours'], index='SiteId', aggfunc='max')
n_df_down_sites_vs_aging_pivot = n_df_down_sites_vs_aging_pivot.reset_index(drop = False)
n_df_down_sites_vs_aging_pivot=n_df_down_sites_vs_aging_pivot.sort_values(by='DownHours', ascending=False)
plot = go.Figure(data=[go.Bar(
    name ='2G Cells',
    x = n_df_down_sites_vs_aging_pivot.SiteId,
    y = n_df_down_sites_vs_aging_pivot['DownHours'],
    text=round(n_df_down_sites_vs_aging_pivot['DownHours'], 1),
   ), 
])
plot.update_layout(
    title={
        'text': "Down Sites With Its Aging (Hours)",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})
plot.update_traces(marker=dict(color = n_df_down_sites_vs_aging_pivot['DownHours'],
                     colorscale='viridis'),textfont_size=16)
plot.update_yaxes(visible=False, showticklabels=False)
st.plotly_chart(plot, use_container_width=True)
#--- End Of Down Sites Pivot And Graphs---#



#---Final Down Cells DataFrame---#

down_cells = n_df_filtered.query('AlarmName == "BCCH MISSING" or AlarmName == "WCDMA CELL OUT OF USE"')
down_cells = down_cells[['SiteId','Office', 'SiteName', 'AlarmName', 'FormattedDatetime', 'final_2G', 'final_3G', 'totalCells', 'DownHours']]
down_cells.rename(columns = {"SiteId": "Site ID", "FormattedDatetime": "Alarm Time", "final_2G": "2G Cells",  "final_3G": "3G Cells",  "totalCells": "Total Cells", "DownHours": "Aging"}, inplace = True)
down_cells.style.background_gradient()
down_cells = down_cells.reset_index(drop=True).sort_values(by='Alarm Time', ascending=True)


#dfi.export(down_cells, 'down_cells.png', max_rows=1,)
#---End of Final Down Cells DataFrame---#


#--- Down Cells Pivot And Graphs---#
cells_fig = px.bar(final_down_cells_without_zero, x='SiteId', y='final_total_down_cells', text_auto='1s', color='SiteId', color_discrete_sequence=px.colors.qualitative.Vivid)

cells_fig.update_layout(
    title={
        'text': "Down Cells Per Sites",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        showlegend=False,
        xaxis_title=None,
        yaxis_title=None,
        )
cells_fig.update_xaxes(tickangle = 55, title_font = {"size": 20},)
cells_fig.update_yaxes(showticklabels=False)
cells_fig.add_annotation(
        x=0.5,
        y=0.8,
        xref="x domain",
        yref="y domain",
        text=f"Total Down Cells <b>{int(final_down_cells_without_zero['final_total_down_cells'].sum())}</b>",
        showarrow=False,
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="#ffffff"
            ),
        align="center",
        borderwidth=2,
        borderpad=4,
        bgcolor="#642ca9",
        opacity=0.6
        )

st.plotly_chart(cells_fig, use_container_width=True)



down_cells_table=n_df_filtered[['SiteId', 'Office', 'FilterIdentifier', 'totalCells','FormattedDatetime', 'Cascaded', 'DownHours']]

down_cells_table = down_cells_table.drop(down_cells_table[(down_cells_table['totalCells'] == 0) | (down_cells_table['totalCells'].isnull())].index)


down_cells_pivot_table = pd.pivot_table(down_cells_table, index=['SiteId', 'Office'], aggfunc={'totalCells': 'max', 'FormattedDatetime': 'min', 'Cascaded':'max', 'DownHours':'max'})
down_cells_pivot_table = down_cells_pivot_table.reset_index(drop = False).sort_values(by='totalCells', ascending=False)


#-- Environmental Alarms--#

environmental_df=n_df_filtered.query('AlarmName == "BASE STATION EXTERNAL ALARM NOTIFICATION"')
environmental_df=environmental_df[["SiteId","Office", "Cascaded", "AlarmInfo","FormattedDatetime", "DownHours"]].sort_values(by='FormattedDatetime', ascending=False)
environmental_df.rename(columns = {"AlarmInfo": "Alarm", "FormattedDatetime": "Alarm Time", "DownHours": "Aging"}, inplace = True)

hub_environmental_df = environmental_df[environmental_df['Cascaded'] > 30].sort_values(by='Cascaded', ascending=False)

environmental_fig = px.bar(hub_environmental_df, y='SiteId', x='Aging', color='Alarm', text='Alarm', barmode='group', orientation='h', color_discrete_sequence=px.colors.qualitative.Prism)
environmental_fig.update_layout(
    title={
        'text': "Hub Environmental Alarms",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        showlegend=False,
        xaxis_title=None,
        yaxis_title=None,
        )

st.plotly_chart(environmental_fig, use_container_width=True)
down_sites = down_sites.reset_index(drop=False)

# pyg_html = pyg.to_html(down_sites)
 
# # Embed the HTML into the Streamlit app
# components.html(pyg_html, height=1000, scrolling=True)

#-- End of Environmental Alarms--#
if st.button('View Data'):
    down_cells
    down_cells_pivot_table
    down_sites
    n_df_down_sites_pivot
    n_df_filtered
    environmental_df
    hub_environmental_df


st.markdown('<div class="seperator"></div>', unsafe_allow_html = True)
colored_header(
        label="",
        description="",
        color_name="red-70",
)
st.text('Curent Uploaded Filters')
tagger_component(
    "",
    n_df['FilterIdentifier'].str[4:].unique(),
    color_name= random.sample(violet_color, len(n_df['FilterIdentifier'].unique()))
)



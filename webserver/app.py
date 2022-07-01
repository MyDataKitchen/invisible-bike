import streamlit as st
import datetime
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
import os
from datetime import time
from datetime import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)


st.set_page_config(layout="wide")
# from PIL import Image
# image = Image.open('logo.png')

# st.sidebar.image(image, use_column_width=True)
st.sidebar.title("INVISIBLE BIKE EXPLORER")
st.sidebar.markdown('---')

df = pd.read_parquet("data.parquet", columns=None, use_threads=True)


# df['datetime'] = pd.to_datetime(df['datetime'])
# df_time = df.groupby(pd.Grouper(key='datetime',freq='1h')).agg({'inPerMinute':'sum', 'outPerMinute':'sum'}).reset_index()
# times = pd.to_datetime(df_time["datetime"], format='%Y-%m-%d %H:%M:%S').dt.strftime('%H:%M').tolist()

# df_districts = df.groupby("district").agg({'outPerMinute':'sum', 'inPerMinute':'sum'}).reset_index()
# districts = df_districts["district"].tolist()
# districts_outPerMinute = df_districts["outPerMinute"].tolist()
# districts_inPerMinute = df_districts["inPerMinute"].tolist()


district = df[['district']].drop_duplicates()['district'].tolist()
available_stations = len(df['stationId'].drop_duplicates().tolist())

city = st.sidebar.selectbox(
  '選取縣市',
  ['台北', '台中'])

date = st.sidebar.date_input(
          "選取日期",
          datetime.date(2022, 6, 19), min_value=datetime.date(2022, 6, 16), max_value=datetime.date(2022, 6, 29))

select_district = st.sidebar.multiselect(
  '選取區域',
  district, district[:])

select_time = st.sidebar.slider(
               "選取時間",
               value=time(12, 00), step=datetime.timedelta(minutes=1))

select_info = st.sidebar.selectbox(
  '選取資料圖層',
    ("可停數量", "可借車輛", "可停空位", "可借用與缺車的對比"))

#

privious_time = (dt.combine(date.today(), select_time) - datetime.timedelta(minutes=1)).time()
previous_df = df[df['time'] == str(privious_time)]

previous_bikes_return = previous_df['inPerMinute'].sum()
previous_bikes_lend = previous_df['outPerMinute'].sum()

df = df[df['district'].isin(select_district)]

df['datetime'] = pd.to_datetime(df['datetime'])
df_time = df.groupby(pd.Grouper(key='datetime',freq='1h')).agg({'inPerMinute':'sum', 'outPerMinute':'sum'}).reset_index()
times = pd.to_datetime(df_time["datetime"], format='%Y-%m-%d %H:%M:%S').dt.strftime('%H:%M').tolist()

df_districts = df.groupby("district").agg({'outPerMinute':'sum', 'inPerMinute':'sum'}).reset_index()
districts = df_districts["district"].tolist()
districts_outPerMinute = df_districts["outPerMinute"].tolist()
districts_inPerMinute = df_districts["inPerMinute"].tolist()




df = df[df['date'] == str(date)]
df = df[df['time'] == str(select_time)]


bikes_return = df['inPerMinute'].sum()
bikes_lend = df['outPerMinute'].sum()

shortage_duration_min_value = df["shortageDuration"].min()
shortage_duration_max_value = df["shortageDuration"].max()


total = df['total'].sum()
emptySpace = df['emptySpace'].sum()
availableSpace = df['availableSpace'].sum()
inPerMinute = df['inPerMinute'].sum()
outPerMinute = df['outPerMinute'].sum()

if select_info == "可停數量":
    size = "total"
    bike_available = st.sidebar.slider('可借用車比例', 0, 100, (0, 100), step=5, key=1)
    df = df[((df['proportion'] >= bike_available[0]) & (df['proportion'] <= bike_available[1]))]
    status = False


elif select_info == "可借車輛":
    size = "availableSpace"
    bike_available = st.sidebar.slider('可借用車比例', 0, 100, (0, 100), step=5, key=1)
    df = df[((df['proportion'] >= bike_available[0]) & (df['proportion'] <= bike_available[1]))]
    status = False

elif select_info == "可停空位":
    size = "emptySpace"
    bike_available = st.sidebar.slider('可借用車比例', 0, 100, (0, 100), step=5, key=1)
    df = df[((df['proportion'] >= bike_available[0]) & (df['proportion'] <= bike_available[1]))]
    status = False

elif select_info == "可借用與缺車的對比":
    size = "total"
    col1, col2 = st.sidebar.columns(2)
    bike_available_1 = col1.slider('可借用車比例', 0, 50, (0, 50), step=5, key=1)
    bike_available_2 = col2.slider('', 50, 100, (50, 100), step=5, key=2)
    df = df[((df['proportion'] >= bike_available_1[0]) & (df['proportion'] <= bike_available_1[1])) | ((df['proportion'] >= bike_available_2[0]) & (df['proportion'] <= bike_available_2[1]))]
    status = True



bike_shortage_duration = st.sidebar.slider('缺車時間長度', int(shortage_duration_min_value), int(shortage_duration_max_value), (int(shortage_duration_min_value), int(shortage_duration_max_value)), step=1, key=2, disabled=status)


df = df[(df['shortageDuration'] >= bike_shortage_duration[0]) & (df['shortageDuration'] <= bike_shortage_duration[1])]

privious_time = (dt.combine(date.today(), select_time) - datetime.timedelta(minutes=1)).time()

col1, col2, col3, col4 = st.columns(4)

col1.metric(label="借用站數量", value=1096,
     delta_color="inverse")

col2.metric(label="服務中的借用站數量", value=available_stations,
     delta_color="off")

col3.metric(label="歸還數量", value=bikes_return, delta=f"{ round(float((bikes_return - previous_bikes_return) / bikes_return), 2) } %",
     delta_color="normal")

col4.metric(label="借出數量", value=bikes_lend, delta=f"{ round(float((bikes_lend - previous_bikes_lend) /  bikes_lend), 2) } %",
     delta_color="normal")

fig = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name="name", hover_data=["total", "emptySpace", "district", "proportion", "availableSpace"], color="proportion", size=size
                        , zoom=12, height=680, opacity=.8, color_continuous_scale="plotly3")
fig.update_layout(mapbox_style="carto-positron")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
st.plotly_chart(fig, use_container_width=True)

st.markdown('---')

st.markdown("#### 目前暫停服務的借用站")
st.text(" \n")
st.text(" \n")




data_matrix = [['借用站編號', '借用站名稱', '區域', '地址'],
               ['500108122', '陽光舊宗路口', '內湖區', '陽光街/舊宗路二段'],
               ['500103044', '大稻埕碼頭', '大同區', '大稻埕疏散門(淡五號水門)外'],
               ['500103044', '成功路三段83號前', '內湖區', '成功路三段83號前']]

fig = ff.create_table(data_matrix)
st.plotly_chart(fig, use_container_width=True)

st.markdown('---')

# bike_shortage_duration = st.sidebar.slider('缺車時間長度', int(shortage_duration_min_value), int(shortage_duration_max_value), (int(shortage_duration_min_value), int(shortage_duration_max_value)), step=1, key=2)

col1, col2 = st.columns(2)

fig = px.line(df_time, x='datetime', y=['inPerMinute', 'outPerMinute'], markers=True, height=500)
newnames = {'inPerMinute': "腳踏車歸還 / 小時", 'outPerMinute': "腳踏車借出 / 小時"}
fig.for_each_trace(lambda t: t.update(name = newnames[t.name],
                                      legendgroup = newnames[t.name],
                                      hovertemplate = t.hovertemplate.replace(t.name, newnames[t.name])
                                     )
)

col1.plotly_chart(fig, use_container_width=True)


fig = go.Figure(data=[
    go.Bar(name='腳踏車歸還 / 區域', x=districts, y=districts_outPerMinute, text=districts_outPerMinute),
    go.Bar(name='腳踏車借出 / 區域', x=districts, y=districts_inPerMinute, text=districts_inPerMinute)
])
# Change the bar mode
fig.update_layout(barmode='group', height=500, xaxis={'categoryorder':'total ascending'})

col2.plotly_chart(fig, use_container_width=True)







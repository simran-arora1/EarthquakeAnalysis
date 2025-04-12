
import streamlit as st
import pandas as pd 
import altair as alt
import datetime
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly.graph_objects as go
import boto3

App_title="🌍Earthquake"


##filters
##time filter
def time_input():
    start_date = st.sidebar.date_input("start_date", datetime.date.today()-relativedelta(months=1))## put default date as one-month earlier than today
    end_dates = st.sidebar.date_input("end_date", datetime.date.today())## select today's date
    end_date=end_dates+datetime.timedelta(days=1)##add one day in order to filter 
    return start_date,end_date
##region filter
def region(df):
    continent_list = sorted(list(df['continent'].dropna().unique()))## get the list of all the continents of source data
    continent_list.insert(0, "All") ## all an option as all
    continent = st.sidebar.selectbox('Continent', continent_list)

    if continent == "All":## After the continent is selected, the country filter will only show the countries that in the selected continents
        country_list = sorted(list(df['country'].dropna().unique()))
    else:
        country_list = sorted(list(df[df['continent'] == continent]['country'].dropna().unique()))

    country_list.insert(0, "All") 
    country = st.sidebar.selectbox('Country', country_list)

    return continent, country 

##magnitude_filter
def magnitude_filter(df,start_date,end_date,continent,country):
    df=df[(df['date']>= start_date)&(df['date']<end_date)]
    if continent != "All":##if the continent and country are selected the slider will only include the range of selected region.
        df= df[df['continent'] == continent]

    if country != "All":
        df= df[df['country'] == country]
    min_mag, max_mag = st.sidebar.slider(
    "Magnitude Range",
    min_value=df['magnitude'].min(),
    max_value=df['magnitude'].max(),
    value=(df['magnitude'].min(), df['magnitude'].max()),
    step=0.1
)   
    return min_mag, max_mag

def tsunami_warning_filter(df):
    if (df["tsunami_warning"] != 0).any():##if the selected date and region has no tsunami warning data, which no record shows tsunami_warning=1. Then this filter will not be shown
        use_mag_filter = st.sidebar.toggle("Tsunami_warning")
        if use_mag_filter:
            df=df[df['tsunami_warning']!=0]
        else:
            df = df
    else:
        df=df
    return df

##Apply the filters 
def filter_data(df,continent,country,start_date,end_date,min_mag,max_mag):
    df_filtered=df[(df['date']>= start_date)&(df['date']<end_date)&(df['magnitude']>=min_mag)&(df['magnitude']<=max_mag)]
    if continent != "All":
        df_filtered = df_filtered[df_filtered['continent'] == continent]
    if country != "All":
        df_filtered = df_filtered[df_filtered['country'] == country]
    return df_filtered


def pie_charts(df):
    df_alert = df[df['alert_level'].notna()]
    color_map = {
    'green': '#B3FFA4',
    'yellow': '#FFECA1',
    'orange': '#F3A942',
    'red': '#E51717',
    'unknown': '#abd2df'
    }
    fig = px.pie(
        df_alert,
        names='alert_level',
        title='Alert Level',
        color='alert_level',
        hole=0.3,
        color_discrete_map=color_map
    )

    st.plotly_chart(fig,use_container_width=True)


def scatter_plots(df):

    fig = px.scatter(
        df,
        x='magnitude',
        y='depth_km',
        color='magnitude', 
        color_continuous_scale='RdYlBu_r',
        color_discrete_map={'red': 'red', 'orange': 'orange', 'blue': 'blue'},
        hover_data=['location', 'time_readable'],
        labels={'depth_km': 'Depth (km)', 'magnitude': 'Magnitude'},
        title='Magnitude vs. Depth'
    )

    fig.update_traces(marker=dict(size=10 ))
    fig.update_layout(showlegend=False)

    st.plotly_chart(fig, use_container_width=True)


def plot_monthly_trend(df,end_date,continent,country,min_mag,max_mag):
    end_date=(end_date+relativedelta(months=1)).replace(day=1)##no matter what date you choose, the filter will select the whole month of the date.
    year_before=(end_date-relativedelta(years=1)).replace(day=1)

    def classify_mag(m):###classifing the magnitude
        if m >= 7:
            return '7+'
        elif m >= 6:
            return '6~6.9'
        else:
            return '<6'

    df['mag_class'] = df['magnitude'].apply(classify_mag)
    df=df[(df['date']>=year_before)&(df['date']<end_date)]
    if continent != "All":
        df = df[df['continent'] == continent]
    if country != "All":
        df = df[(df['country'] == country)]
    df=df[(df['magnitude']>=min_mag)&(df['magnitude']<=max_mag)]
    ##line plot
    totals_line=df.groupby(['year','month'])['id'].nunique().reset_index()
    totals_line['year_month'] = totals_line['year'].astype(str) + "-" + totals_line['month'].astype(str).str.zfill(2)+'-01'

    ##stack barchart
    stacked = df.groupby(['year','month', 'mag_class'])['id'].count().reset_index()
    stacked['year_month'] =  stacked ['year'].astype(str) + "-" +  stacked ['month'].astype(str).str.zfill(2)+'-01'
    pivot = stacked.pivot(index='year_month', columns='mag_class', values='id').fillna(0)

    colors = {
    '<6': '#abd2df',     
    '6~6.9': '#ffd699',  
    '7+': '#b13c54'      
}
    ##plots
    fig = go.Figure()
    ##stacked barchart
    for mag_class in pivot.columns:
        fig.add_trace(go.Bar(
            x=pivot.index,
            y=pivot[mag_class],
            name=f"M {mag_class}",
            marker_color=colors.get(mag_class, None)
        ))
    ##lines
    fig.add_trace(go.Scatter(
        x=totals_line['year_month'],
        y=totals_line['id'],
        mode='lines+markers',
        name='Number of Earthquakes',
        line=dict(color='black', width=2)
    ))

    fig.update_layout(
        title="Monthly Earthquake Count by Magnitude(Recent 12 month)",
        barmode='stack',
        xaxis_title='Month',
        yaxis_title='Earthquake Count',
        xaxis_tickangle=-45,
        legend_title='Magnitude Class'
    )

    st.plotly_chart(fig, use_container_width=True)

def display_map_and_table(df):
    if df.empty:
        st.info("No data available for the selected filters.")
        return
    df = df.sort_values(by="magnitude")
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="magnitude",
        size="magnitude",
        hover_name="location",
        hover_data=["time_readable", "depth_km", "country", "continent", "alert_level"],
        zoom=1,
        height=500,
        color_continuous_scale=["lightblue","orange",'Red'],
    )
    fig.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig, use_container_width=True)

##numebr of earthquake by countries
def country_rank(df):
    country_table = (
    df.groupby(["country",'continent'])["id"]
    .nunique()
    .reset_index(name="Earthquake Count")
    .sort_values(by="Earthquake Count", ascending=False).set_index('country')
    .head(10)
)
    st.subheader('Earthquake Hotspots')
    st.dataframe(country_table)

def recent_7days(df,continent,country,end_date,min_mag,max_mag):
    start_date=end_date-datetime.timedelta(days=7) ## select 7 days before the end_date
    df_f=df[(df['date']>= start_date)&(df['date']<end_date)&(df['magnitude']>=min_mag)&(df['magnitude']<=max_mag)]
    if continent != "All":
        df_f = df_f[df_f['continent'] == continent]

    if country != "All":
        df_f= df_f[df_f['country'] == country]
    fig = px.scatter(
        df_f,
        x='date',
        y='magnitude',
        color='magnitude', 
        color_continuous_scale='RdYlBu_r',
        color_discrete_map={'red': 'red', 'orange': 'orange', 'blue': 'blue'},
        hover_data=['location', 'time_readable'],
        labels={'depth_km': 'Depth (km)', 'magnitude': 'Magnitude'},
        title='Recent 7 days'
    )

    fig.update_traces(marker=dict(size=10 ))
    fig.update_layout(showlegend=False)

    st.plotly_chart(fig, use_container_width=True)


def main():

    st.set_page_config(App_title,page_icon='🌍',layout="wide")
    st.title("🌍Earthquake")
    left_side, mid_side,right_side= st.columns([1.5,2,1])

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("earthquakes")

    response = table.scan()
    items = response['Items']

    while 'LastEvaluatedKey' in response:
        lastEvaluatedKey = response['LastEvaluatedKey']
        response = table.scan(
        ExclusiveStartKey=lastEvaluatedKey) 
        items.extend(response['Items'])

    df = pd.DataFrame(items)
    print(df.shape[0])

    df['date']=pd.to_datetime(df['date']).dt.date
    float_columns = ['rms_amplitude', 'magnitude', 'mmi_intensity', 'latitude', 'longitude', 'azimuthal_gap', 'depth_km', 'felt_reports']
    df[float_columns] = df[float_columns].astype(float)

    ##filters
    continent,country=region(df)
    start_date,end_date=time_input()
    min_mag,max_mag=magnitude_filter(df,start_date,end_date,continent,country)

    filtered_df=filter_data(df,continent,country,start_date,end_date,min_mag,max_mag)

    
    if filtered_df.empty:
        st.info("No data available for the selected filters.")
        return
    filtered_df=tsunami_warning_filter(filtered_df)

    ##Layout
    with st.container():
        with left_side:
            col1, col2,col3,col4= st.columns(4)
            with col1:  
                ##max magnitude kpi
                row_index = filtered_df[filtered_df['magnitude'] == filtered_df['magnitude'].max()].iloc[0]
                max_country = row_index['country']
                max_mag = filtered_df['magnitude'].max()
                if max_mag >= 7: ## if the mangitude is above 7, the card will be red 
                    bg_color = "#ffcccc"  # red 
                elif max_mag >= 6:## if the mangitude is below 7 and above 6, the card will be orange 
                    bg_color = "#ffd699"  # orange
                else:#other wise, the color just same as other cards
                    bg_color = "#f0f2f6"  
                st.markdown(f"""
                <div style="background-color:{bg_color};padding:10px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                    Max Magnitude
                </div>
                <div style="font-size:20px; ">
                    {max_country}: {filtered_df['magnitude'].max()}
                </div>
                <div style="font-size:20px; ">
                FeltReports:{ row_index['felt_reports']}
                </div>
                <div style="font-size:20px; ">
                    Depth(KM): {row_index['depth_km']}
                </div>
                <div style="font-size:20px; ">
                    { row_index['time_readable']}
                </div>
                </div>
                """, unsafe_allow_html=True)


            
            with col2:
                ##Total record
                st.markdown(f"""
                <div style="background-color:#f0f2f6;padding:10px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                    Total Earthquakes
                </div>
                <div style="font-size:20px; ">
                    {filtered_df['id'].nunique()}
                </div>
                </div>
                """, unsafe_allow_html=True)
                
                ##Depth
                st.markdown(f"""
                <div style="background-color:#f0f2f6;padding:10px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                    Average Depth
                </div>
                <div style="font-size:20px; ">
                    {round(filtered_df['depth_km'].mean(),2)}
                </div>
                </div>
                """, unsafe_allow_html=True)



            with col3:
                ##FeltReports
                st.markdown(f"""
                <div style="background-color:#f0f2f6;padding:10px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                    Average FeltReports
                </div>
                <div style="font-size:20px; ">
                    {round(filtered_df['felt_reports'].mean(),2)}
                </div>
                </div>
                """, unsafe_allow_html=True)
                ##tsunami warning
                st.markdown(f"""
                <div style="background-color:#f0f2f6;padding:10px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                    Tsunami Warnings
                </div>
                <div style="font-size:20px; ">
                    {round(filtered_df['tsunami_warning'].sum(),2)}
                </div>
                </div>
                """, unsafe_allow_html=True)

            with col4:
            ##history record
                row_history_index = df[df['magnitude'] == df['magnitude'].max()].iloc[0]
                his_max_country = row_history_index['country']
                st.markdown(f"""
                <div style="background-color:#f0f2f6;padding:5px;border-radius:10px;text-align:center">
                <div style="font-size:20px; font-weight:bold; margin-bottom:5px">
                History Max Magnitude
                </div>
                <div style="font-size:20px; ">
                    {his_max_country}: {df['magnitude'].max()}
                </div>
                <div style="font-size:20px; ">
                FeltReports:{ row_history_index ['felt_reports']}
                </div>
                <div style="font-size:20px; ">
                    Depth(KM): {row_history_index ['depth_km']}
                </div>
                <div style="font-size:20px; ">
                    { row_history_index ['time_readable']}
                </div>
                </div>
                """, unsafe_allow_html=True)
            recent_7days(df,continent,country,end_date,min_mag,max_mag)
    

    with mid_side:
        display_map_and_table(filtered_df)

    with right_side:
        pie_charts(filtered_df)
     ##map
    with st.container():
        left_side, mid_side,right_side= st.columns([1.5,2,1])
        with mid_side:
            plot_monthly_trend(df,end_date,continent,country,min_mag,max_mag)
        with right_side:
            country_rank(filtered_df)
        with left_side:
            scatter_plots(filtered_df)
    st.subheader('Detailed info')
    st.dataframe(filtered_df[['magnitude','location','time_readable',
    'depth_km','country',
    'continent','alert_level','tsunami_warning','detail_url']].sort_values('time_readable',ascending=False).set_index("time_readable").head(50),use_container_width=True)


if __name__ == "__main__":
    main()                                                               

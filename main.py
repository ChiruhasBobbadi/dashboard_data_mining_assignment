import dash
import dash_core_components as dcc
from dash import html
import dash_html_components as html
from dash.dependencies import Output, Input, State
from datetime import date
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import fbprophet as Prophet
from google.oauth2 import service_account  # pip install google-auth
import pandas_gbq  # pip install pandas-gbq
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime
from dateutil import parser

credentials = service_account.Credentials.from_service_account_file(
    '/Users/chiruhasbobbadi/PycharmProjects/dash/assets/btcprice255-3bc9a00142bc.json')
project_id = 'btcprice255'  # make sure to change this with your own project ID

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])

myTitle = dcc.Markdown(children='# Temperature Analysis')
california_graph_title = dcc.Markdown('## Temperature in California')
myGraph = dcc.Graph(figure={})
# predGraphTitle = dcc.Markdown('## Temperature prediction in Santa Clara County')
# myGraph2 = dcc.Graph(figure={})
high = dcc.Markdown('## Counties with highest temperature in California during last 5 years')
myGraph3 = dcc.Graph(figure={})
scatter = dcc.Markdown('## Scatter plot of daily temperature and mean temperature in San jose city')
myGraph4 = dcc.Graph(figure={})
usa = dcc.Markdown('## Average of last 5 years Temperature in USA by state')
myGraph5 = dcc.Graph(figure={})

dropDown = dcc.Dropdown(options=['Daily Temperature vs Time', 'Maximum Temperature Vs Time'],
                        value='Daily Temperature vs Time',
                        clearable=False)

app.layout = dbc.Container(
    [myTitle, california_graph_title, myGraph, dropDown, high, myGraph3, scatter, myGraph4,
     usa, myGraph5, dcc.Store(id='myData', data=[], storage_type="memory")])
df_sj = pd.DataFrame();

## functions
us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "Virgin Islands": "VI",
    "Country Of Mexico": "MX",
    "District Of Columbia": "COL"
}


def plot_single_station_daily_temp(daily_temp_data, plot_fields=pd.DataFrame(), station_name="San Jose"):
    daily_plot_data = []
    for index, row in plot_fields.iterrows():
        daily_plot_data = (daily_plot_data +
                           [go.Scatter(
                               x=daily_temp_data['date_local'],
                               y=daily_temp_data[row['field_name']],
                               name=row['plot_label'],
                               marker=dict(
                                   # Constant color scale for plotting temp to use for all stations
                                   cmin=-22,  # -22°F corresponds to -30°C (very cold, to most)
                                   cmax=122,  # 122°F corresponds to 50°C (very hot, to most)
                                   color=daily_temp_data[row['field_name']],
                                   # colorscale = 'BlueReds',
                                   colorscale=[[0, 'rgb(0, 0, 230)'], [0.5, 'rgb(190, 190, 190)'],
                                               [1, 'rgb(230, 0, 0)']],
                                   symbol=row['marker_symbol']
                               ),
                               line=dict(
                                   color=row['line_color']
                               ),
                               mode=row['plot_mode']
                           )]
                           )

    daily_plot_layout = go.Layout(
        title=dict(
            text=(station_name + ' Daily Temperature'),
            xref="paper",
            x=0.5
        ),
        yaxis=dict(title='Temperature (°F)')
    )
    return go.Figure(daily_plot_data, daily_plot_layout)


def nametoabb(name):
    return us_state_to_abbrev[name]


# # # call back function
@app.callback(
    Output('myData', 'data'),
    Input(dropDown, component_property='value')
)
def update_graph1(user_input):
    print("data updated")
    # I recommend running the SQL in Good Cloud to make sure it works
    # before running it here in your Dash App.

    df_sql = """SELECT *FROM `bigquery-public-data.epa_historical_air_quality.temperature_daily_summary`
    WHERE date_local BETWEEN '2015-01-01' AND '2022-01-01'
    """
    df = pd.read_gbq(df_sql, project_id=project_id, dialect='standard', credentials=credentials)
    #print(len(df))

    df_temp = df.drop(
        ["site_num", "parameter_code", "poc", "datum", "latitude", "longitude", "method_name",
         "date_of_last_change",
         "parameter_name", "sample_duration", "pollutant_standard",
         "units_of_measure", "event_type", "observation_count", "observation_percent", "aqi", "method_code",
         "local_site_name", "address", "cbsa_name"
         ], axis=1)

    return df_temp.to_dict('records')




@app.callback(
    Output(myGraph, component_property='figure'),
    Input('myData', 'data'),
    Input(dropDown, component_property='value')
)
def graph1(df_temp,user_input):
    print("g1 triggerd")
    df_temp = pd.DataFrame(df_temp)
    ## sorting by state name
    df = df_temp['state_name']=='California'
    df_cal = df_temp[df]
    fig = px.bar(data_frame=df_cal, x='date_local', y='arithmetic_mean',
                 labels={'date_local': 'TIME', 'arithmetic_mean': 'Daily Temperature'})
    if 'Temperature vs Time' == user_input:
        fig = px.bar(data_frame=df_cal, x='date_local', y='arithmetic_mean',
                     labels={'date_local': 'TIME', 'arithmetic_mean': 'Daily Temperature'})
    elif 'Maximum Temperature Vs Time' == user_input:
        fig = px.bar(data_frame=df_cal, x='date_local', y='first_max_value',
                     labels={'date_local': 'TIME', 'first_max_value': 'Maximum Temperature'})

    return fig



##todo
# @app.callback(
#     Output(myGraph2, 'figure'),
#     Input(dropDown, 'value')
# )
#
# def update_graph2(input):
#     print("Triggered")
#     df_sql = """SELECT *FROM `bigquery-public-data.epa_historical_air_quality.temperature_daily_summary`
#         WHERE date_local BETWEEN '2015-01-01' AND '2022-01-01' AND state_name='California'
#         """
#
#     df = pd.read_gbq(df_sql, project_id=project_id, dialect='standard', credentials=credentials)
#     print(len(df))
#
#     df_temp = df.drop(
#         ["site_num", "parameter_code", "poc", "datum", "latitude", "longitude", "method_name", "date_of_last_change",
#          "parameter_name", "sample_duration", "pollutant_standard",
#          "units_of_measure", "event_type", "observation_count", "observation_percent", "aqi", "method_code",
#          "local_site_name", "address", "cbsa_name"
#          ], axis=1)
#
#
#     county = 'Santa Clara'
#     filter_case = 'arithmetic_mean'
#     period_to_forecast = 45
#
#     # Filter data
#     df = df_temp['county_name'] == 'Santa Clara'
#
#     df_sj = df_temp[df]
#     df_sj =  df_sj.rename(columns={"date": "ds", filter_case: "y"})
#     graph=''
#     df_sj['ds'] = pd.to_datetime(df_sj['date_local'], infer_datetime_format=True)
#     df_sj = df_sj[df_sj['ds'] > "2020-02-01"]
#     df_sj['y'] = df_sj['y'].astype(int)
#     df_sj = df_sj[['y', 'ds']]
#     pred = Prophet(yearly_seasonality=False, daily_seasonality=False)
#     pred.fit(df_sj)
#     future = pred.make_future_dataframe(periods=60)
#     forecast = pred.predict(future)
#     graph = pred.plot(forecast)
#     return graph

@app.callback(
    Output(myGraph3, 'figure'),
    Input('myData', 'data')
)
def update_graph3(temp):
    print("g3 triggered")
    df_temp =pd.DataFrame(temp)
    df_all = df_temp
    Latest_date = df_all["date_local"].max()
    latest_info = df_all[df_all["date_local"] == Latest_date]

    top_temp = latest_info.sort_values('first_max_value', ascending=False)
    tt = top_temp.groupby("county_name")['first_max_value'].max()
    tt = tt.reset_index()

    fig = px.bar(top_temp, x=top_temp['county_name'], y=top_temp['first_max_value'])

    return fig

# function to update scatter plot
@app.callback(
    Output(myGraph4, 'figure'),
    Input('myData', 'data')
)
def update_graph4(df):
    print("g4 Triggered")
    df_temp = pd.DataFrame(df)
    ## sorting by state name
    df = df_temp['state_name'] == 'California'
    df_temp = df_temp[df]
    dsj = df_temp['city_name'] == 'San Jose'
    df_sj = df_temp[dsj]
    print(len(df_sj))
    chosen_station_name = "San Jose"
    daily_temp_plot_fields = pd.DataFrame.from_records(
        columns=['field_name', 'plot_label', 'marker_symbol', 'line_color',
                 'plot_mode'],
        data=[
            ('arithmetic_mean', 'Avg', 'circle', None, 'markers'),
            ('first_max_value', 'Max', 'triangle-up', None, 'markers'),
        ]
    )
    return plot_single_station_daily_temp(df_sj, daily_temp_plot_fields, chosen_station_name)



# usa plot

@app.callback(
    Output(myGraph5, 'figure'),
    Input('myData', 'data')
)
def update_graph5(val):
    print("G5 Triggered")
    df_temp = pd.DataFrame(val)

    # invert the dictionary
    abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

    df_temp["code"] = df_temp["state_name"].apply(nametoabb)
    df_temp = df_temp.groupby(['state_name', 'code'])['arithmetic_mean'].mean()
    df_temp = df_temp.reset_index()

    fig = px.choropleth(df_temp,  # Input Pandas DataFrame
                  locations="code",  # DataFrame column with locations
                  color="arithmetic_mean",  # DataFrame column with color values
                  hover_name="code",  # DataFrame column hover info
                  locationmode='USA-states')  # Set to plot as US States
    fig.update_layout(
            title_text='Last 5 years Average temperature in USA by state',  # Create a Title
            geo_scope='usa',  # Plot only the USA instead of globe
        )
    return fig

if __name__ == '__main__':
    app.run_server(debug=False)

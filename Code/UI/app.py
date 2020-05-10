import dash
from flask import Flask
import dash_core_components as dcc
import dash_html_components as html
from pyspark.sql.functions import col
from dash.dependencies import Input, Output
from pyspark.sql import SparkSession, functions
import colorlover as cl
import pandas as pd
import dash_table
import pandas as pd
import plotly.graph_objs as go

import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'data_operations')))
import data_operations

#Colour scale for candlestick plot of OHLC data
colorscale = cl.scales['9']['qual']['Paired']

"""
# Command to run the file (makes connection to cassandra database)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 app.py

"""

app = Flask(__name__)
cluster_seeds = ['127.0.0.1']
spark = SparkSession.builder.appName('Stock Analysis').config('spark.cassandra.connection.host',\
                                                                      ','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

# Our cassandra keyspace
KEYSPACE_NAME = 'stocks'

all_ohlcv_data = data_operations.get_all_ohlcv_data_from_db(tablename="ohlcv", KEYSPACE=KEYSPACE_NAME)
all_pca = data_operations.get_all_pca_data_from_db(tablename="all_pca", KEYSPACE=KEYSPACE_NAME)

all_pca_kmeans_with_sentiment = data_operations.get_all_pca_with_sentiment_data_from_csv(folder_name="../k_means_cluster_analysis/Results_with_sentiment/pca_folder_cl3/")
all_pca_kmeans_without_sentiment = data_operations.get_all_pca_without_sentiment_data_from_csv(folder_name="../k_means_cluster_analysis/Results_without_sentiment/pca_folder_cl3/")

all_pca_kmedoids_with_sentiment = data_operations.get_all_pca_with_sentiment_data_from_csv(folder_name="../K_medoids_cluster_analysis/Results_with_sentiment/pca_folder_cl3/")
all_pca_kmedoids_without_sentiment = data_operations.get_all_pca_without_sentiment_data_from_csv(folder_name="../K_medoids_cluster_analysis/Results_without_sentiment/pca_folder_cl3/")

# Integrating Flask on top of Dash server
dashapp = dash.Dash(__name__, server= app, url_base_pathname='/')

#Switching to multiple tabs. Allowed callbacks outside of Main Layout
dashapp.config['suppress_callback_exceptions'] = True

# Creating necessary lists
sectors = ['Select Sector', 'Industrials', 'Health Care', 'Information Technology', 'Consumer Discretionary', 'Utilities', 'Financials', 'Materials', 'Real Estate', 'Consumer Staples', 'Energy']

year_list = ["2018_Q2", "2018_Q3", "2018_Q4", "2019_Q1", "2019_Q2", "2019_Q3", "2019_Q4", "2020_Q1", "Entire_Period"]
marks_dict = {i: year_list[i-1] for i in range(1, len(year_list)+1)}

only_quarters = ["2018_Q2", "2018_Q3", "2018_Q4", "2019_Q1", "2019_Q2", "2019_Q3", "2019_Q4", "2020_Q1"]
only_quarters_dict = {i: only_quarters[i-1] for i in range(1, len(only_quarters)+1)}

quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"]

features = ["Select Feature", "Open_Close Values", "Trend indicator - EMA", "Volatility indicator - ATR", "Volume indicator - ADOSC", "Momemtum indicator - RSI, ROC"]
sentiment_option = ["Select Sentiment Option", "With News Sentiment", "Without News Sentiment"]

profile_table_col_names = ['Unnamed: 0', 'Overall', 'KM_1', 'KM_2', 'KM_3']

clustering_method_list = ["K-means", "K-medoids"]
data_list = ["Min-Max", "Top 5 portfolios over 2 years", "Portfolio generated quarterwise"]
sentiment_list = ["With Sentiment", "Without Sentiment"]

chart_type_list = ["Bar Chart", "Line Chart", "Cluster EDA"]

portfolio_generation_two_year_with_sentiment_table_columns = ['Unnamed: 0', "Sharpe Ratio", "Portfolio Annualised Return", \
"Portfolio Annualised_Volatility", "Stock Portfolio", "Stocks Weight Allocation", "Portfolio Tag"]

static_bar_chart_feature_list = ["Open", "Close", "daily_fluctuation", "High", "volume"]
static_line_chart_feature_list = ["Close", "ClosersChange", "Open", "OpenCloseVariation", "Volume"]

#Tab styling
tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

#Webapp layout
with app.app_context():
    dashapp.layout = html.Div(
    children=[
        html.Div([
            # --header section
            html.Div([
                html.H2('StockZilla'),
                html.H3('One Stop Solution For Portfolio Generation Using Unsupervised Learning')
            ], style={'text-align': 'center', 'width': '100%', 'display': 'inline-block', 'vertical-align': 'middle','font-family':'Helvetica', 'color': 'white'}),
            dcc.Tabs(id="tabs", value='viewohlcvplots', children=[
                dcc.Tab(label='View OHLC Plots', value='viewohlcvplots', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='View Feature Plots', value='viewfeatureplots', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='Cluster Visualization', value='clustervisualization', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label="Portfolio Generation", value="portfoliogeneration", style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label="Static Vizualization", value="static", style=tab_style, selected_style=tab_selected_style)
            ], style=tabs_styles),
            html.Div(id='tabs-content')
            ],style={'width': '100%','background-position': 'initial initial', 'background-repeat': 'initial initial'},
        )
     ], style={'background-image': 'url("./static/images/background/background_2.gif")'})

#Render pageview according to tabs
@dashapp.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'viewohlcvplots':
        return html.Div([
        html.Div([
        html.Div([
            html.P(
                'Sector Type',
                className="control_label",
                style={
                'textAlign': 'center', 
                'color': 'white'
                }
            ),
            html.Div(
                dcc.Dropdown(
                    id='sectortype-column',
                    options=[{'label': i, 'value': i} for i in sectors],
                    value='Select Sector',
                ), style={'width': '40%', 'display': 'inline-block'}
            ),
            html.Div(
                dcc.Dropdown(
                    id='company-column',
                    value='Select Company',
                ), style={'width': '40%', 'display': 'inline-block'}
            ) 
        ],
        style={'width': '75%', 'display': 'inline-block'}),

         ],
        style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(34, 34, 34)',
        'padding': '10px 5px'
        }),

        html.Div([
            
            html.Div([
                dcc.Graph(id='ohlc_time_sector_median')
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),
            html.Div([
                dcc.Graph(id='company_graph')
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),
                 ], className='row',style={'padding': '10px 5px'}),


            html.Div([
                html.P(
                    'Drag the slider to change the time period',
                    className="control_label",
                    style={
                        'textAlign': 'center',
                        "color": "white"
                    }
                ),
                dcc.Slider(
                    id="time-period-slider",
                    min=1,
                    max=9,
                    value=9,
                    marks=marks_dict,
                    step=None
                ),
            ], style={'width': '50%', 'margin-left': '25%', 'padding': '20px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(34, 34, 34)'}),

        ], style={'margin-top': '10px'})

    elif tab == 'viewfeatureplots':
        return html.Div([
        html.Div([
        html.Div([
            html.P(
                'Sector Type',
                className="control_label",
                style={
                'textAlign': 'center', 
                'color': 'white'
                }
            ),
            html.Div(
                dcc.Dropdown(
                    id='feature_sectortype-column',
                    options=[{'label': i, 'value': i} for i in sectors],
                    value='Select Sector',
                ), style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(
                dcc.Dropdown(
                    id='feature_company-column',
                    value='Select Company',
                ), style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(
                dcc.Dropdown(
                    id='feature-column',
                    options=[{'label': i, 'value': i} for i in features],
                    value='Select Feature',
                ), style={'width': '50%', 'display': 'inline-block'}
            ) 
        ],
        style={'width': '90%', 'display': 'inline-block'}),

         ],
        style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(34, 34, 34)',
        'padding': '10px 5px'
        }),

        html.Div([
            
            html.Div([
                dcc.Graph(id='feature_ohlc_time_sector_median')
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),

            html.Div([
                dcc.Graph(id='feature_company_graph')
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),
                 ], className='row',style={'padding': '10px 5px'}),


            html.Div([
                html.P(
                    'Drag the slider to change the time period',
                    className="control_label",
                    style={
                        'textAlign': 'center',
                        "color": "white"
                    }
                ),
                dcc.Slider(
                    id="feature_time-period-slider",
                    min=1,
                    max=9,
                    value=9,
                    marks=marks_dict,
                    step=None
                ),
            ], style={'width': '50%', 'margin-left': '25%', 'padding': '20px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(34, 34, 34)'}),

        ], style={'margin-top': '10px'})
    
    elif tab == 'clustervisualization':
        
        return html.Div([

        html.Div([
                html.P(
                    'Drag the slider to change the time period',
                    className="control_label",
                    style={
                        'textAlign': 'center',
                        "color": "white"
                    }
                ),
                dcc.Slider(
                    id="cluster_time-period-slider",
                    min=1,
                    max=len(only_quarters),
                    value=3,
                    marks=only_quarters_dict,
                    step=None
                ),
            ], style={'width': '50%', 'margin-left': '25%', 'padding': '20px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(34, 34, 34)'}),

        html.Div([

                html.Div(
                    dash_table.DataTable(
                        id='quarter_profile_table',
                        columns=[{"name": i, "id": i} for i in profile_table_col_names],
                        data = [],
                    ), style={'width': '80%',  'margin-left': '10%', "margin-top": '1%', 'display': 'inline-block'}
                ),

                html.Div([
                            html.P(
                                'The profile table displayed above represents the distribution of the mean values of the features of the formed cluster.',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '80%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            )
                        ], style={'display': 'inline-block', 'width': '49%', 'margin-left':'18%'}, className='six columns'),

                html.Div(
                    dcc.Dropdown(
                        id='sentiment-option-column',
                        options=[{'label': i, 'value': i} for i in sentiment_option],
                        value=sentiment_option[1],
                    ),style={'width': '50%', 'margin-left': '25%', "margin-top": "1%", 'padding': '20px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(34, 34, 34)'}),

                html.Div([
                    html.Div(
                    dcc.Dropdown(
                        id='cluster_clustering_method-column',
                        options=[{'label': i, 'value': i} for i in clustering_method_list],
                        value=clustering_method_list[0],
                    ), style={'width': '30%',  'margin-left': '10%', "margin-top": '1%', 'display': 'inline-block'}
                    ),
                    html.Div(
 
                        dcc.Dropdown( 
                            id='cluster_feature-column',
                            options=[{'label': i, 'value': i} for i in features],
                            value='Select Feature',
                        ), style={'width': '30%',  'padding-left': '17.5%', "margin-top": '1%', 'display': 'inline-block'}
                    ), 
                ]),
            ]),
        
        html.Div([
            
            html.Div([
                dcc.Graph(id='cluster_graph',
                    hoverData={'points': [{'customdata': 'AMZN'}]})
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),

            html.Div([
                dcc.Graph(id='cluster_feature_graph')
                     ]
                     , style={'display': 'inline-block', 'width': '47%', 'margin-left': '1.2%'}, className='six columns'),
                 ], className='row',style={'padding': '10px 5px'})
            

        ], style={'margin-top': '10px'})

    elif tab == 'portfoliogeneration':

        return html.Div([

            html.Div(
                html.Label(["Clustering Method",
                dcc.Dropdown(
                    id='clustering_method-column',
                    options=[{'label': i, 'value': i} for i in clustering_method_list],
                    value=clustering_method_list[0],
                    style={'color':'black'}
                    )]), style={'width': '20%', 'display': 'inline-block', 'color': 'white', 'backgroundColor':'black'}
            ),

            html.Div(
                html.Label(["Type",
                dcc.Dropdown(
                    id='data-column',
                    options=[{'label': i, 'value': i} for i in data_list],
                    value=data_list[0],
                    style={'color':'black'}
                )]), style={'width': '20%', 'display': 'inline-block', 'color': 'white', 'backgroundColor':'black'}
            ),

            html.Div(
                html.Label(["Sentiment",
                dcc.Dropdown(
                    id='sentiment-column',
                    options=[{'label': i, 'value': i} for i in sentiment_list],
                    value=sentiment_list[0],
                    style={'color':'black'}
                )]), style={'width': '20%', 'display': 'inline-block','color': 'white', 'backgroundColor':'black'}
            ),
            html.Div([
                html.P(
                    'The portfolios suggested by K-Medoids are recommended',
                    className="six columns",
                    style={'textAlign': 'center', 'width': '85%', 'margin-left': '5%', 'margin-top': '5%',
                           'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                           'backgroundColor': 'rgb(250, 250, 250)', 'color': 'red', 'font-family': 'Helvetica', 'font-size': '18px'}
                )
            ], style={'display': 'inline-block', 'width': '40%'}, className='six columns'),
            html.Div(id='portfolio_generation_content'),
        ])   

    elif tab == 'static':

        return html.Div([

            html.Div(
                dcc.Dropdown(
                    id='chart_type-column',
                    options=[{'label': i, 'value': i} for i in chart_type_list],
                    value=chart_type_list[0],
                ), style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(id='static_content'),
        ])     

###########################################################################################
########################################OHLCV PLOTS########################################
###########################################################################################

def candlestick_trace(df, title):

    candlestick = {
        "x":df.time,
        "open":df["open"],
        "high":df["high"],
        "low":df["low"],
        "close":df["close"],
        "type":"candlestick",
        "increasing":{'line': {'color': colorscale[0]}},
        "decreasing":{'line': {'color': colorscale[1]}},
        "name": title
    }

    return {'data': [candlestick],
        'layout': {
                'title': title,
                'xaxis': dict(autoscale=True,
                              type='date'),
                'yaxis': dict(autoscale=True)

            }
        }

# Return sector wise median ohlcv over all quarters
@dashapp.callback(
    Output('ohlc_time_sector_median', 'figure'),
    [Input('sectortype-column', 'value'),
    Input('time-period-slider', 'value')
     ])

def ohlc_time_sector_median(xaxis_column_name, time_period):
    if(xaxis_column_name == "Select Sector"):
        title = "No Sector Selected"
        ohlc_time_sector_median_df = None
    else:        
        if(time_period == 9):
            ohlc_time_sector_median_df = all_ohlcv_data[all_ohlcv_data['sector'] == xaxis_column_name]
            ohlc_time_sector_median_df = data_operations.get_all_ohlc_time_median(ohlc_time_sector_median_df)

            title = 'OHLCV for {} for {}'.format(xaxis_column_name, "entire time period")

        else:

            start_date = quarter_list[time_period-1]
            end_date = quarter_list[time_period]

            ohlc_time_sector_median_df = all_ohlcv_data[(all_ohlcv_data['sector'] == xaxis_column_name) & (all_ohlcv_data['time'] >= start_date) & (all_ohlcv_data['time'] < end_date)]
            ohlc_time_sector_median_df = data_operations.get_all_ohlc_time_median(ohlc_time_sector_median_df)

            title = 'OHLCV for {} for {}'.format(xaxis_column_name, year_list[time_period-1])

    return candlestick_trace(ohlc_time_sector_median_df, title)

@dashapp.callback(
    Output('company_graph', 'figure'),
    [Input('sectortype-column', 'value'),
    Input('company-column', 'value'),
    Input('time-period-slider', 'value')
     ])    

def ohlc_company(sector_name, company_name, time_period):
    
    company_symbol = company_name.split(" - ")[0]

    if(company_name == "Select Company"):
        title = "No Company Selected"
    else:
        
        if(time_period == 9):
            ohlc_company_df = all_ohlcv_data[all_ohlcv_data['symbol'] == company_symbol]
            title = 'OHLCV for {} for {}'.format(company_name, "entire time period")
        else:
            start_date = quarter_list[time_period-1]
            end_date = quarter_list[time_period]
                
            ohlc_company_df = all_ohlcv_data[(all_ohlcv_data['symbol'] == company_symbol) & (all_ohlcv_data['time'] >= start_date) & (all_ohlcv_data['time'] < end_date)]
            title = 'OHLCV for {} for {}'.format(company_name, year_list[time_period-1])

    return candlestick_trace(ohlc_company_df, title)
    

@dashapp.callback(
    Output('company-column', 'options'),
    [Input('sectortype-column', 'value')
     ])

def display_company_list(sector_name):
    company_list = data_operations.get_company_lst(sector_name)
    return [{'label': 'Select Company', 'value': 'Select Company'}] + [{'label': i, 'value': i} for i in company_list] 


###########################################################################################
####################################FEATURE OHLCV PLOTS####################################
###########################################################################################

def line_graph(df, title, feature_name):

    if (feature_name == "Feature"):
        return {'layout': {
                'title': title,
                }
            }
    if(feature_name == "Momemtum"):
        line_graph_1 = go.Scatter(
            x = df["time"],
            y = df[feature_name+"_1"],
            mode = 'lines',
            name = "RSI"
        )
        line_graph_2 = go.Scatter(
            x = df["time"],
            y = df[feature_name+"_2"],
            mode = 'lines',
            name = "ROC"
        )
        return {'data': [line_graph_1, line_graph_2],
                'layout': {
                        'title': title,

                    }
                }
    elif(feature_name == "Open_Close"):
        line_graph_1 = go.Scatter(
            x = df["time"],
            y = df[feature_name+"_1"],
            mode = 'lines',
            name = "Open"
        )
        line_graph_2 = go.Scatter(
            x = df["time"],
            y = df[feature_name+"_2"],
            mode = 'lines',
            name = "Close"
        )
        return {'data': [line_graph_1, line_graph_2],
                'layout': {
                        'title': title,
                    }
                }

    line_graph = go.Scatter(
        x = df["time"],
        y = df[feature_name],
        mode = 'lines',
        name = feature_name
    )

    return {'data': [line_graph],
            'layout': {
                'title': title,
                }
            }

@dashapp.callback(
    Output('feature_ohlc_time_sector_median', 'figure'),
    [Input('feature_sectortype-column', 'value'),
    Input('feature_time-period-slider', 'value'),
    Input('feature-column', 'value')
     ])

def ohlc_time_sector_median(xaxis_column_name, time_period, feature):

    feature_name = feature.split()[0]

    if(xaxis_column_name == "Select Sector"):
        title = "No Sector Selected"
    else:      
        feature_ohlc_time_sector_median_df = all_ohlcv_data[all_ohlcv_data['sector'] == xaxis_column_name]
        feature_ohlc_time_sector_median_df = data_operations.get_all_ohlc_time_median(feature_ohlc_time_sector_median_df)
        feature_ohlc_time_sector_median_df = data_operations.get_feature_df(df=feature_ohlc_time_sector_median_df, feature=feature_name)  
        if(time_period == 9):
            
            title = 'Plot of {} for {} for {}'.format(xaxis_column_name, "entire time period", feature)

        else:

            start_date = quarter_list[time_period-1]
            end_date = quarter_list[time_period]
            feature_ohlc_time_sector_median_df = feature_ohlc_time_sector_median_df[(feature_ohlc_time_sector_median_df['time'] >= pd.to_datetime(start_date)) & (feature_ohlc_time_sector_median_df['time'] < pd.to_datetime(end_date))]
            title = 'Plot of {} for {} for {}'.format(xaxis_column_name, year_list[time_period-1], feature)

    return line_graph(feature_ohlc_time_sector_median_df, title, feature_name)

# Return company wise features

@dashapp.callback(
    Output('feature_company_graph', 'figure'),
    [Input('feature_company-column', 'value'),
    Input('feature_time-period-slider', 'value'),
    Input('feature-column', 'value')
     ])

def feature_company(company_name, time_period, feature):

    company_symbol = company_name.split(" - ")[0]
    feature_name = feature.split()[0]
    
    if(feature == "Select Feature"):
        title = "No Feature Selected"
    else:
        if(time_period == 9):
            ohlcv_feature_df = data_operations.get_feature_df(all_ohlcv_data, company_symbol, feature_name)
            title = 'Plot of {} for {} for {}'.format(feature_name, company_name, "entire time period")
        else:
            start_date = quarter_list[time_period-1]
            end_date = quarter_list[time_period]

            ohlcv_feature_df = data_operations.get_feature_df(all_ohlcv_data, company_symbol, feature_name)
            ohlcv_feature_df = ohlcv_feature_df[(ohlcv_feature_df['time'] >= start_date) & (ohlcv_feature_df['time'] < end_date)]
            title = 'Plot of {} for {} for {}'.format(feature_name, company_name, year_list[time_period-1])

    return line_graph(ohlcv_feature_df, title, feature_name)


@dashapp.callback(
    Output('feature_company-column', 'options'),
    [Input('feature_sectortype-column', 'value')
     ])

def display_company_list(sector_name):
    company_list = data_operations.get_company_lst(sector_name)
    return [{'label': 'Select Company', 'value': 'Select Company'}] + [{'label': i, 'value': i} for i in company_list] 

###########################################################################################
#######################################CLUSTER PLOTS#######################################
###########################################################################################

@dashapp.callback(
    Output('cluster_graph', 'figure'),
    [Input('cluster_time-period-slider', 'value'),
    Input('sentiment-option-column', 'value'),
    Input('cluster_clustering_method-column', 'value')
    ])


def cluster_graph(time_period, senti_option, clustering_method):

    if(senti_option == sentiment_option[1]):
        if(clustering_method == clustering_method_list[0]):
            df = all_pca_kmeans_with_sentiment
        elif(clustering_method == clustering_method_list[1]):
            df = all_pca_kmedoids_with_sentiment
    if(senti_option == sentiment_option[1]):
        if(clustering_method == clustering_method_list[0]):
            df = all_pca_kmeans_without_sentiment
        elif(clustering_method == clustering_method_list[1]):
            df = all_pca_kmedoids_without_sentiment

    title = "Cluster graph for {}".format(year_list[time_period-1])

    if(time_period != 9):
        df = df[df['Quarter']==time_period]

    cluster0 = dict(
            x=df[df["label"]==0]['PC_0'],
            y=df[df["label"]==0]["PC_1"],
            text=df[df["label"]==0]["symbol"],
            customdata=df[df["label"]==0]["symbol"],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name="cluster0"
        )
    cluster1 = dict(
            x=df[df["label"]==1]['PC_0'],
            y=df[df["label"]==1]["PC_1"],
            text=df[df["label"]==1]["symbol"],
            customdata=df[df["label"]==1]["symbol"],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name="cluster1"
        )
    cluster2 = dict(
            x=df[df["label"]==2]['PC_0'],
            y=df[df["label"]==2]["PC_1"],
            text=df[df["label"]==2]["symbol"],
            customdata=df[df["label"]==2]["symbol"],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name="cluster2"
        )
    cluster3 = dict(
            x=df[df["label"]==3]['PC_0'],
            y=df[df["label"]==3]["PC_1"],
            text=df[df["label"]==3]["symbol"],
            customdata=df[df["label"]==3]["symbol"],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name="cluster3"
        )
    cluster4 = dict(
            x=df[df["label"]==4]['PC_0'],
            y=df[df["label"]==4]["PC_1"],
            text=df[df["label"]==4]["symbol"],
            customdata=df[df["label"]==4]["symbol"],
            mode='markers',
            marker={
                'size': 15,
                'opacity': 0.5,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name="cluster4"
        )


    return {
        'data': [cluster0, cluster1, cluster2, cluster3, cluster4],
        'layout': dict(
            xaxis={
                'title': "PC_0",
            },
            yaxis={
                'title': "PC_1",
            },
            margin={'l': 40, 'b': 30, 't': 10, 'r': 0},
            height=450,
            hovermode='closest'
        )
    }

@dashapp.callback(
    dash.dependencies.Output('cluster_feature_graph', 'figure'),
    [dash.dependencies.Input('cluster_graph', 'hoverData'),
     dash.dependencies.Input('cluster_feature-column', 'value'),
     dash.dependencies.Input('cluster_time-period-slider', 'value')])

def cluster_company_graph(hoverData, feature, time_period):
    df = all_ohlcv_data

    company_symbol = hoverData['points'][0]['customdata']
        
    feature_name = feature.split()[0]

    dff = df[df['symbol'] == company_symbol]

    ohlcv_feature_df = data_operations.get_feature_df(df, company_symbol, feature_name)

    if(time_period != 9):

        start_date = quarter_list[time_period-1]
        end_date = quarter_list[time_period]
        
        ohlcv_feature_df = ohlcv_feature_df[(ohlcv_feature_df['time'] >= start_date) & (ohlcv_feature_df['time'] < end_date)]
        title = 'Plot of {} for {} for {}'.format(feature_name, company_symbol, year_list[time_period-1])

    else:
        title = 'Plot of {} for {} for {}'.format(feature_name, company_symbol, "entire time period")

    return line_graph(ohlcv_feature_df, title, feature_name)


@dashapp.callback(
    [Output("quarter_profile_table", "data")],
    [Input('cluster_time-period-slider', 'value'),
    Input('sentiment-option-column', "value")]
)

def generate_quarter_profile_table(time_period, senti_option):

    if(senti_option == sentiment_option[1]):
        df = data_operations.get_profile_with_sentiment_from_csv(folder_name="../k_means_cluster_analysis/Results_with_sentiment/Profiles_qtrwise_cl3/", time_period=time_period)
    elif(senti_option == sentiment_option[2]):
        df = data_operations.get_profile_with_sentiment_from_csv(folder_name="../k_means_cluster_analysis/Results_without_sentiment/Profiles_qtrwise_cl3/", time_period=time_period)

    return [df.to_dict("records")]

###########################################################################################
#################################PORTFOLIO GENERATION######################################
###########################################################################################

def generate_quarter_profile_table(clustering_method, data_method, sentiment_method):

    if(sentiment_method == sentiment_list[0]):
        if(clustering_method == clustering_method_list[0]):
            file_name = "../Portfolio_Generation/K-Means/with_sentiment/Static_Content/"+data_method+"/"+data_method+".csv"
        elif(clustering_method == clustering_method_list[1]):
            file_name = "../Portfolio_Generation/K-Medoids/with_sentiment/Static_Content/"+data_method+"/"+data_method+".csv"
    
    elif(sentiment_method == sentiment_list[1]):
        if(clustering_method == clustering_method_list[0]):
            file_name = "../Portfolio_Generation/K-Means/without_sentiment/Static_Content/"+data_method+"/"+data_method+".csv"
        elif(clustering_method == clustering_method_list[1]):
            file_name = "../Portfolio_Generation/K-Medoids/without_sentiment/Static_Content/"+data_method+"/"+data_method+".csv"

    df = pd.read_csv(file_name)

    return [df.to_dict("records")]

@dashapp.callback(
    Output("portfolio_generation_content", "children"),
    [Input('clustering_method-column', 'value'),
    Input('data-column', "value"),
    Input('sentiment-column', "value")]
)

def image_content(clustering_method, data_method, sentiment_method):

    root_path = "./static/"
    if(sentiment_method == sentiment_list[0]):
        if(clustering_method == clustering_method_list[0]):
            folder_name = root_path+"Portfolio_Generation/K-Means/with_sentiment/Static_Content/"+data_method+"/"
        elif(clustering_method == clustering_method_list[1]):
            folder_name = root_path+"Portfolio_Generation/K-Medoids/with_sentiment/Static_Content/"+data_method+"/"
    
    elif(sentiment_method == sentiment_list[1]):
        if(clustering_method == clustering_method_list[0]):
            folder_name = root_path+"Portfolio_Generation/K-Means/without_sentiment/Static_Content/"+data_method+"/"
        elif(clustering_method == clustering_method_list[1]):
            folder_name = root_path+"Portfolio_Generation/K-Medoids/without_sentiment/Static_Content/"+data_method+"/"

    df = pd.read_csv(folder_name+data_method+".csv")

    # "Min-Max"
    if(data_method == data_list[0]):

        df = df[['0', '1', '2', '3', '4']]
        df.columns = ["Sharpe Ratio", "Annualised Return", "Annualised Volatility", "Stock Portfolio", "Stocks Weight Allocation"]

        pg_df = df.to_dict("records")
        return (html.Div(
                        dash_table.DataTable(
                            id='portfolio_generation_dashtable',
                            columns=[{"name": i, "id": i} for i in df.columns],
                            data = pg_df,
                            style_data={'whiteSpace': 'normal'},
                            css=[{'selector': '.dash-cell div.dash-cell-value',
                                              'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                        ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
                    ),

                    html.Div([
                        html.Div([
                            html.P(
                                'The plot on the left generates results considering Portfolio returns, Volatility and Sharpe Ratio for the 25000 different random weights assigned to each of the stocks. \
                                The highest Sharpe Ratio portfolio is displayed with "red" star sign and minimum Volatility is displayed with a "green" star on the plot. \
                                The rest all portfolios are plotted in "blue" colour. The darker the shade of blue, higher the Sharpe ratio. ',


                                className="six columns",
                                style={'textAlign': 'center', 'width': '90%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            ],style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
    
                          html.Div([

                            html.P(
                                'The plot on the right indicates the best distribution of the given stock considering the volatility and anualised returns. \
                                For any stock to be considered safe, it needs to have low volatility and high returns and must be lying on the line connecting the red and green star.',


                                className="six columns",
                                style={'textAlign': 'center', 'width': '85%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            )
                            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                            
                        ], style={'display': 'inline-block', 'width': '100%'}, className='row'),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Sharpe Ratio using Effective Frontier',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"min_max_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Sharpe Ratio on Individual',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"min_max_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ))

    # "Top 5 portfolios over 2 years"
    elif(data_method == data_list[1]):

        df = df[["Sharpe Ratio", "Portfolio Annualised Return", "Portfolio Annualised_Volatility", "Stock Portfolio", "Stocks Weight Allocation", "Portfolio Tag"]]
        df.columns = ["Sharpe Ratio", "Annualised Return", "Annualised Volatility", "Stock Portfolio", "Stocks Weight Allocation", "Portfolio Tag"]

        pg_df = df.to_dict("records")

        return (html.Div(
                        dash_table.DataTable(
                            id='portfolio_generation_dashtable',
                            columns=[{"name": i, "id": i} for i in df.columns],
                            data = pg_df,
                            style_data={'whiteSpace': 'normal'},
                            css=[{'selector': '.dash-cell div.dash-cell-value',
                                              'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                        ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
                    ),

                    html.Div([
                        html.Div([
                            html.P(
                                'The plot on the left generates results considering Portfolio returns, Volatility and Sharpe Ratio for the 25000 different random weights assigned to each of the stocks. \
                                The highest Sharpe Ratio portfolio is displayed with "red" star sign and minimum Volatility is displayed with a "green" star on the plot. \
                                The rest all portfolios are plotted in "blue" colour. The darker the shade of blue, higher the Sharpe ratio. ',


                                className="control_label",
                                style={'textAlign': 'center', 'width': '90%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
        
                            html.Div([
                            html.P(
                                'The plot on the right indicates the best distribution of the given stock considering the volatility and anualised returns. \
                                For any stock to be considered safe, it needs to have low volatility and high returns and must be lying on the line connecting the red and green star.',


                                className="control_label",
                                style={'textAlign': 'center', 'width': '85%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            )
                            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                            
                        ], style={'display': 'inline-block', 'width': '100%'}, className='six columns'),

                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Efficient Frontier for the first portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_1_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual stock returns and volatility for the first portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_1_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                        html.Div([
                        html.Div([
                            html.P(
                                'Plot of Efficient Frontier for the second portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_2_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual stock returns and volatility for the second portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_2_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                    html.Div([
                    html.Div([
                        html.P(
                            'Plot of Efficient Frontier for the third portfolio in top 5',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"top5_3_ef.jpg",
                                 style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                    html.Div([
                        html.P(
                            'Plot of Individual stock returns and volatility for the third portfolio in top 5',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"top5_3_ind.jpg", style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                ], className='row',
                    style={
                        'padding': '10px 5px',
                        'margin-left': '3%'
                    } ),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Efficient Frontier for the fourth portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_4_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual stock returns and volatility for the fourth portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_4_ind.jpg", style={'width': '98%', 'height': '600px', "opacity": "100%"})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Efficient Frontier for the fifth portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_5_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual stock returns and volatility for the fifth portfolio in top 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"top5_5_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } )
                )    
    
    # "Portfolio generated quarterwise"
    elif(data_method == data_list[2]):

        df = df[["Quarter Start Date_x", "Sharpe Ratio", "Portfolio Annualised Return", "Portfolio Annualised_Volatility", "Stock Portfolio", "Stocks Weight Allocation"]]
        df.columns = ["Quarter Start Date", "Sharpe Ratio", "Annualised Return", "Annualised Volatility", "Stock Portfolio", "Stocks Weight Allocation"]
        pg_df = df.to_dict("records")

        return (html.Div(
                        dash_table.DataTable(
                            id='portfolio_generation_dashtable',
                            columns=[{"name": i, "id": i} for i in df.columns],
                            data = pg_df,
                            style_data={'whiteSpace': 'normal'},
                            css=[{'selector': '.dash-cell div.dash-cell-value',
                                              'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                        ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
                    ),

                    html.Div([
                        html.Div([
                            html.P(
                                'The plot on the left generates results considering Portfolio returns, Volatility and Sharpe Ratio for the 25000 different random weights assigned to each of the stocks. \
                                The highest Sharpe Ratio portfolio is displayed with "red" star sign and minimum Volatility is displayed with a "green" star on the plot. \
                                The rest all portfolios are plotted in "blue" colour. The darker the shade of blue, higher the Sharpe ratio. ',


                                className="control_label",
                                style={'textAlign': 'center', 'width': '90%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'The plot on the right indicates the best distribution of the given stock considering the volatility and anualised returns. \
                                For any stock to be considered safe, it needs to have low volatility and high returns and must be lying on the line connecting the red and green star.',


                                className="control_label",
                                style={'textAlign': 'center', 'width': '85%', 'margin-left': '5%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            )
                            ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                            
                        ], style={'display': 'inline-block', 'width': '100%'}, className='six columns'),

                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 1',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q1_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 1',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q1_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                        html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 2',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q2_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 2',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q2_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                    html.Div([
                    html.Div([
                        html.P(
                            'Plot of Effective Frontier Stock Portfolio for Quarter 3',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"q3_ef.jpg",
                                 style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                    html.Div([
                        html.P(
                            'Plot of Individual Stock Portfolio for Quarter 3',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"q3_ind.jpg", style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                ], className='row',
                    style={
                        'padding': '10px 5px',
                        'margin-left': '3%'
                    } ),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 4',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q4_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 4',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q4_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),



                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q5_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 5',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q5_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 6',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q6_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 6',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q6_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 7',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q7_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 7',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q7_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),


                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Effective Frontier Stock Portfolio for Quarter 8',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q8_ef.jpg",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Individual Stock Portfolio for Quarter 8',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"q8_ind.jpg", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } )
                )
###########################################################################################
#################################STATIC VISUALIZATION######################################
###########################################################################################

@dashapp.callback(
    Output("static_content", "children"),
    [Input('chart_type-column', 'value')]
)

def static_layout(chart_type_method):

    if(chart_type_method == chart_type_list[0]):    
        
        return (html.Div([
            html.Div(
                dcc.Dropdown(
                    id='static_bar_chart_feature-column',
                    options=[{'label': i, 'value': i} for i in static_bar_chart_feature_list],
                    value=static_bar_chart_feature_list[0],
                ), style={'width': '50%', 'display': 'inline-block'}
            ),

            html.Div(id='static_bar_chart_content')
            ])  )

    elif(chart_type_method == chart_type_list[1]):

        return (html.Div([
            html.Div(
                dcc.Dropdown(
                    id='static_line_chart_feature-column',
                    options=[{'label': i, 'value': i} for i in static_line_chart_feature_list],
                    value=static_line_chart_feature_list[0],
                ), style={'width': '50%', 'display': 'inline-block'}
            ),

            html.Div(id='static_line_chart_content')
            ])  
        )

    elif(chart_type_method == chart_type_list[2]):

        return (html.Div([
            html.Div(
                    dcc.Dropdown(
                        id='cluster_eda_sentiment-option-column',
                        options=[{'label': i, 'value': i} for i in sentiment_list],
                        value=sentiment_list[1],
                    ),style={'width': '50%', 'margin-left': '25%', "margin-top": "1%", 'padding': '20px 20px 20px 20px',
                      'borderBottom': 'thin lightgrey solid',
                      'backgroundColor': 'rgb(34, 34, 34)'}),

                html.Div(
                    dcc.Dropdown(
                        id='cluster_eda_clustering_method-column',
                        options=[{'label': i, 'value': i} for i in clustering_method_list],
                        value=clustering_method_list[0],
                    ), style={'width': '30%',  'margin-left': '55%', "margin-top": '1%', 'display': 'inline-block'}
                ),

            html.Div(id='static_cluster_eda_content')
            ]) )

@dashapp.callback(
    Output("static_bar_chart_content", "children"),
    [Input('static_bar_chart_feature-column', 'value')]
)

def generate_static_bar_charts(feature_name):

    folder_name = "./static/images/BarCharts/"+feature_name+"/"
    return (
        html.Div([
        html.Div([
                    html.P(
                        'Plot of Quarter 1',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q1.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 2',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q2.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 3',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q3.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 4',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q4.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 5',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q5.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 6',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q6.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 7',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q7.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),

        html.Div([
                    html.P(
                        'Plot of Quarter 8',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=folder_name+feature_name+"_Q8.png", style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '48%'}, className='six columns'),
            ])
        )

@dashapp.callback(
    Output("static_line_chart_content", "children"),
    [Input('static_line_chart_feature-column', 'value')]
)

def generate_static_line_charts(feature_name):

    static_line_chart_feature_list = ["Close", "ClosersChange", "Open", "OpenCloseVariation", "Volume"]

    file_name = "./static/images/LineCharts/"+feature_name+"/"+feature_name+".png"
    return (
        html.Div([
        html.Div([
                    html.P(
                        'Plot of All Quarters',
                        className="control_label",
                        style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                               'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                               'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                    ),
                    html.Img(src=file_name, style={'width': '98%', 'height': '600px'})
                ], style={'display': 'inline-block', 'width': '70%'}, className='six columns')
        ])
    )

@dashapp.callback(
    Output("static_cluster_eda_content", "children"),
    [Input('cluster_eda_sentiment-option-column', 'value'),
    Input('cluster_eda_clustering_method-column', 'value')]  
)

def generate_static_cluster_eda(sentiment_method, clustering_method):

    root_path = "../"

    if(sentiment_method == sentiment_list[0]):
        if(clustering_method == clustering_method_list[0]):
            file_name = root_path+"k_means_cluster_analysis/companies_clstr_membership_withsenti_cl3.csv"
        elif(clustering_method == clustering_method_list[1]):
            file_name = root_path+"K_medoids_cluster_analysis/companies_clstr_membership_withsenti_cl3.csv"
    
    elif(sentiment_method == sentiment_list[1]):
        if(clustering_method == clustering_method_list[0]):
            file_name = root_path+"k_means_cluster_analysis/companies_clstr_membership_withoutsenti_cl3.csv"
        elif(clustering_method == clustering_method_list[1]):
            file_name = root_path+"K_medoids_cluster_analysis/companies_clstr_membership_withoutsenti_cl3.csv"

    df = pd.read_csv(file_name)

    df = df[['Unnamed: 0', 'symbol', 'sector']]
    df.columns = ["Sr. No.", 'symbol', 'sector']

    pg_df = df.to_dict("records")
    return (html.Div(
            dash_table.DataTable(
                        id='portfolio_generation_dashtable',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data = pg_df,
                        style_data={'whiteSpace': 'normal'},
                        css=[{'selector': '.dash-cell div.dash-cell-value',
                                          'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                    ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
            ))

#Run Flask server
if __name__ == '__main__':
    app.run()

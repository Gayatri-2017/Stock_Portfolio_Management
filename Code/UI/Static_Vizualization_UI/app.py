import dash
from flask import Flask
import dash_core_components as dcc
import dash_html_components as html
from pyspark.sql.functions import col
from dash.dependencies import Input, Output
import pandas as pd
import dash_table
import pandas as pd
import plotly.graph_objs as go

"""
# Command to run the file (makes connection to cassandra database)

spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 app.py

"""



app = Flask(__name__)

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

features = ["Select Feature", "Open_Close Values", "Trend indicator - EMA", "Volatility indicator - ATR", \
"Volume indicator - ADOSC", "Momemtum indicator - RSI, ROC"]

sentiment_option = ["Select Sentiment Option", "With News Sentiment", "Without News Sentiment"]

profile_table_col_names = ['Unnamed: 0', 'Overall', 'KM_1', 'KM_2', 'KM_3']

clustering_method_list = ["K-means", "K-medoids"]
data_list = ["Min-Max", "Top 5 portfolios over 2 years", "Portfolio generated quarterwise"]
sentiment_list = ["With Sentiment", "Without Sentiment"]

# portfolio_generation_two_year_with_sentiment_table_columns = ['Unnamed: 0', "Sharpe Ratio", "Portfolio Annualised Return", \
# "Portfolio Annualised_Volatility", "Stock Portfolio", "Stocks Weight Allocation", "Portfolio Tag"]
portfolio_generation_two_year_with_sentiment_table_columns = ['Unnamed: 0', "Sharpe Ratio", "Annualised Return", \
"Annualised Volatility", "Stock Portfolio", "Stocks Weight Allocation", "Portfolio Tag"]

file_path_kmeans_cluster4_with_senti = "../../Portfolio_Genration/portfolio_with_news_sentiment/cluster_result_4/"
pg_df = pd.read_csv(file_path_kmeans_cluster4_with_senti+"two-year-with-senti.csv")
pg_df.columns = portfolio_generation_two_year_with_sentiment_table_columns
portfolio_generation_two_year_with_sentiment_table_columns = portfolio_generation_two_year_with_sentiment_table_columns[1:]

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
                html.H2('Stock Portfolio Analysis'),
            ], style={'text-align': 'center', 'width': '100%', 'display': 'inline-block', 'vertical-align': 'middle','font-family':'Helvetica', 'color': 'white'}),
            dcc.Tabs(id="tabs", value='portfoliogeneration', children=[
                dcc.Tab(label='View OHLC Plots', value='viewohlcvplots', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='View Feature Plots', value='viewfeatureplots', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label='Cluster Visualization', value='clustervisualization', style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label="Portfolio Generation", value="portfoliogeneration", style=tab_style, selected_style=tab_selected_style),
                dcc.Tab(label="Static Plots", value="staticplots", style=tab_style, selected_style=tab_selected_style)
            ], style=tabs_styles),
            html.Div(id='tabs-content')
            ],style={'width': '100%','background-position': 'initial initial', 'background-repeat': 'initial initial'},
        )
    ], style={'background-image': 'url("./static/images/background.jpeg")'})

#Render pageview according to tabs
@dashapp.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'portfoliogeneration':

        return html.Div([

            html.Div(
                dcc.Dropdown(
                    id='clustering_method-column',
                    options=[{'label': i, 'value': i} for i in clustering_method_list],
                    value=clustering_method_list[0],
                ), style={'width': '20%', 'display': 'inline-block'}
            ),

            html.Div(
                dcc.Dropdown(
                    id='data-column',
                    options=[{'label': i, 'value': i} for i in data_list],
                    value=data_list[0],
                ), style={'width': '20%', 'display': 'inline-block'}
            ),

            html.Div(
                dcc.Dropdown(
                    id='sentiment-column',
                    options=[{'label': i, 'value': i} for i in sentiment_list],
                    value=sentiment_list[0],
                ), style={'width': '20%', 'display': 'inline-block'}
            ),

            html.Div(id='portfolio_generation_content'),
        ])

def generate_quarter_profile_table(clustering_method, data_method, sentiment_method):

    # clustering_method_list = ["K-means", "K-medoids"]
    # data_list = ["Min-Max", "Top 5 portfolios over 2 years", "Portfolio generated quarterwise"]
    # sentiment_list = ["With Sentiment", "Without Sentiment"]

    print("*"*80)
    print("Inside generate_quarter_profile_table")

    print("clustering_method = ", clustering_method, " data_method = ", data_method, " sentiment_list ", sentiment_method)

    if(sentiment_method == sentiment_list[0]):
        
        if(clustering_method == clustering_method_list[0]):
            
            if(data_method == data_list[0]):

                folder_name="../k_means_cluster_analysis/Results_with_sentiment/min_max/"
                file_name = folder_name + "min_max.csv"
                df = pd.read_csv(file_name)
        
            if(data_method == data_list[1]):

                folder_name="../k_means_cluster_analysis/Results_with_sentiment/top5_2year/"
                file_name = folder_name + "top5_2year.csv"
                df = pd.read_csv(file_name)

            if(data_method == data_list[2]):

                folder_name="../k_means_cluster_analysis/Results_with_sentiment/quarterwise_portfolios/"
                file_name = folder_name + "quarterwise_portfolios.csv"
                df = pd.read_csv(file_name)

        elif(clustering_method == clustering_method_list[1]):

            if(data_method == data_list[0]):

                folder_name="../k_medoids_cluster_analysis/Results_with_sentiment/min_max/"
                file_name = folder_name + "min_max.csv"
                df = pd.read_csv(file_name)
        
            if(data_method == data_list[1]):

                folder_name="../k_medoids_cluster_analysis/Results_with_sentiment/top5_2year/"
                file_name = folder_name + "top5_2year.csv"
                df = pd.read_csv(file_name)

            if(data_method == data_list[2]):

                folder_name="../k_medoids_cluster_analysis/Results_with_sentiment/quarterwise_portfolios/"
                file_name = folder_name + "quarterwise_portfolios.csv"
                df = pd.read_csv(file_name)


    elif(sentiment_method == sentiment_list[1]):
        
        if(clustering_method == clustering_method_list[0]):
            
            if(data_method == data_list[0]):

                folder_name="../k_means_cluster_analysis/Results_without_sentiment/min_max/"
                file_name = folder_name + "min_max.csv"
                df = pd.read_csv(file_name)
        
            elif(data_method == data_list[1]):

                folder_name="../k_means_cluster_analysis/Results_without_sentiment/top5_2year/"
                file_name = folder_name + "top5_2year.csv"
                df = pd.read_csv(file_name)

            elif(data_method == data_list[2]):

                folder_name="../k_means_cluster_analysis/Results_without_sentiment/quarterwise_portfolios/"
                file_name = folder_name + "quarterwise_portfolios.csv"
                df = pd.read_csv(file_name)

        elif(clustering_method == clustering_method_list[1]):

            if(data_method == data_list[0]):

                folder_name="../k_medoids_cluster_analysis/Results_without_sentiment/min_max/"
                file_name = folder_name + "min_max.csv"
                df = pd.read_csv(file_name)
        
            elif(data_method == data_list[1]):

                folder_name="../k_medoids_cluster_analysis/Results_without_sentiment/top5_2year/"
                file_name = folder_name + "top5_2year.csv"
                df = pd.read_csv(file_name)

            elif(data_method == data_list[2]):

                folder_name="../k_medoids_cluster_analysis/Results_without_sentiment/quarterwise_portfolios/"
                file_name = folder_name + "quarterwise_portfolios.csv"
                df = pd.read_csv(file_name)


    print("df = \n", df)

    return [df.to_dict("records")]

@dashapp.callback(
    [Output("portfolio_generation_content", "children")],
    [Input('clustering_method-column', 'value'),
    Input('data-column', "value"),
    Input('sentiment-column', "value")]
)

def image_content(clustering_method, data_method, sentiment_method):

    # clustering_method_list = ["K-means", "K-medoids"]
    # data_list = ["Min-Max", "Top 5 portfolios over 2 years", "Portfolio generated quarterwise"]
    # sentiment_list = ["With Sentiment", "Without Sentiment"]

    if(sentiment_method == sentiment_list[0]):
        if(clustering_method == clustering_method_list[0]):
            folder_name="./static/images/with_sentiment/k_means/"
        elif(clustering_method == clustering_method_list[1]):
            folder_name="./static/images/with_sentiment/k_medoids/"
    
    elif(sentiment_method == sentiment_list[1]):
        if(clustering_method == clustering_method_list[0]):
            folder_name="./static/images/without_sentiment/k_means/"
        elif(clustering_method == clustering_method_list[1]):
            folder_name="./static/images/without_sentiment/k_medoids/"

    pg_df = generate_quarter_profile_table(clustering_method, data_method, sentiment_method)

    if(data_method == data_list[0]):

        return (html.Div(
                        dash_table.DataTable(
                            id='portfolio_generation_dashtable',
                            columns=[{"name": i, "id": i} for i in portfolio_generation_two_year_with_sentiment_table_columns],
                            data = pg_df,
                            style_data={'whiteSpace': 'normal'},
                            css=[{'selector': '.dash-cell div.dash-cell-value',
                                              'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                        ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
                    ),

                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Image 1 ',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag1.png",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Image 2',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag2.png", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ))

        # , style={'margin-top': '10px'}

    elif(data_method == data_list[1]):

        return (html.Div(
                        dash_table.DataTable(
                            id='portfolio_generation_dashtable',
                            columns=[{"name": i, "id": i} for i in portfolio_generation_two_year_with_sentiment_table_columns],
                            data = pg_df,
                            style_data={'whiteSpace': 'normal'},
                            css=[{'selector': '.dash-cell div.dash-cell-value',
                                              'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}]
                        ), style={'width': '80%',  'margin-left': '1%', "margin-top": '1%', "margin-right": '1%', "margin-bottom": '1%', 'display': 'inline-block'}
                    ),

                    html.Div([
                        html.Div([
                            html.P(
                                'Plot of Image 1 ',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag1.png",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Image 2',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag2.png", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                        html.Div([
                        html.Div([
                            html.P(
                                'Plot of Image 1 ',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag1.png",
                                     style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                        html.Div([
                            html.P(
                                'Plot of Image 2',
                                className="control_label",
                                style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                       'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                       'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                            ),
                            html.Img(src=folder_name+"imag2.png", style={'width': '98%', 'height': '600px'})
                        ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                    ], className='row',
                        style={
                            'padding': '10px 5px',
                            'margin-left': '3%'
                        } ),

                    html.Div([
                    html.Div([
                        html.P(
                            'Plot of Image 1 ',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"imag1.png",
                                 style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),

                    html.Div([
                        html.P(
                            'Plot of Image 2',
                            className="control_label",
                            style={'textAlign': 'center', 'width': '50%', 'margin-left': '25%',
                                   'padding': '10px 10px 10px 10px', 'borderBottom': 'thin lightgrey solid',
                                   'backgroundColor': 'rgb(250, 250, 250)', 'font-family': 'Helvetica', 'font-size': '18px'}
                        ),
                        html.Img(src=folder_name+"imag2.png", style={'width': '98%', 'height': '600px'})
                    ], style={'display': 'inline-block', 'width': '49%'}, className='six columns'),
                ], className='row',
                    style={
                        'padding': '10px 5px',
                        'margin-left': '3%'
                    } )
                )    

                








#Run Flask server
if __name__ == '__main__':
    app.run()




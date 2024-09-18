import dash
import logging
import json
import numpy as np
import pandas as pd
from datetime import datetime
import utils.ura_utils as utils
from db import SqliteManager
import plotly.express as px
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
from flask import Flask, jsonify, request, render_template

#### START #####

#Setup logging
logger = logging.getLogger(__name__)
utils.set_logger()

#Read data
trxn_cnt_by_datetime_df = pd.read_csv("data/trxn_cnt_by_datetime.csv")
trxn_cnt_by_district_df = pd.read_csv("data/trxn_cnt_by_district.csv")

#Initialize app
app = Flask(__name__)

@app.route("/")
def index():
    return "Testing homepage."

dashboard = dash.Dash(server = app, routes_pathname_prefix= "/dash/")
dashboard.layout = html.Div("This is a test Dashboard.")

trxn_cnt_by_datetime = px.bar(trxn_cnt_by_datetime_df, x="contract_quarter_year", y="transaction_count")
trxn_cnt_by_district = px.bar(trxn_cnt_by_district_df, x="district", y="transaction_count")

dashboard.layout = html.Div(
    children=[
        html.H1(children = '''
            Summary statistics of private property transactions in Singapore.
        
        '''
        ),
        html.Div(
            [
            html.H3(
                children='Transaction counts broken down by quarter.'
                ),

            html.Div(
                children='''
                Transaction counts by quarter
                '''
                ),
            dcc.Graph(
                id='Transaction counts by quarter',
                figure=trxn_cnt_by_datetime
                )

            ]
        ),
        html.Div(
            [
                html.H3(children ='''
                        Transaction counts broken down by district.
                        '''
                        ),
                html.Div(children='''
                         Transaction counts by district
                         '''
                         ),
                dcc.Graph(
                    id ='Transaction counts by district',
                    figure = trxn_cnt_by_district
                )
            ]

        )


    ]

)


if __name__ == "__main__":
    dashboard.run_server(host = "0.0.0.0", port = 8080,debug=True)

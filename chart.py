import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd

df = pd.read_csv('data_analytics/datasets/processed/pepeusdt_5m_28_june.csv')
df = df[df['is_kline_closed'] == True]
df['trendline'] = ''

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(
        id='candlestick-chart',
        figure={
            'data': [
                go.Candlestick(
                    x=df['kline_start_time'],
                    open=df['open_price'],
                    high=df['high_price'],
                    low=df['low_price'],
                    close=df['close_price'],
                    name='Candlesticks'
                )
            ],
            'layout': {
                'title': 'Interactive Candlestick Chart',
                'xaxis_title': 'Date',
                'yaxis_title': 'Price',
                'xaxis_rangeslider_visible': True,  
                'width': 1200,  
                'height': 800,  
            }
        }
    ),
    html.Div(id='selected-points-info', style={'margin-top': 20}),
    html.Div(id='input-container', children=[
        dcc.Input(id='trendline-input', type='text', placeholder='Enter trendline value'),
        html.Button('Add', id='add-button', n_clicks=0, style={'margin-left': '10px'})
    ], style={'display': 'none'}),  
    html.Button('Save to CSV', id='save-button', n_clicks=0, style={'margin-top': 20}),
    html.Div(id='save-output', style={'margin-top': 20}),
    html.Div(id='dataframe-output', style={'margin-top': 20})
])


selected_points = []


@app.callback(
    Output('input-container', 'style'),
    [Input('candlestick-chart', 'clickData')]
)
def display_input_container(clickData):
    if clickData is not None and 'points' in clickData:
        return {'display': 'block'}  
    return {'display': 'none'}  

@app.callback(
    Output('dataframe-output', 'children'),
    [Input('add-button', 'n_clicks')],
    [dash.dependencies.State('trendline-input', 'value'),
     Input('candlestick-chart', 'clickData')]
)
def add_trendline_value(n_clicks, trendline_value, clickData):
    if n_clicks > 0 and trendline_value:
        
        clicked_index = clickData['points'][0]['pointIndex']
        
        
        df.at[clicked_index, 'trendline'] = trendline_value
        
        
        updated_row = df.iloc[clicked_index]
        return html.Pre(f'Updated Row:\n{updated_row.to_string()}\n\nFull DataFrame:\n{df.to_string()}')

    return 'Enter a trendline value and click on a candle to annotate.'

@app.callback(
    Output('save-output', 'children'),
    [Input('save-button', 'n_clicks')]
)
def save_to_csv(n_clicks):
    if n_clicks > 0:
        df.to_csv('annotated_data.csv', index=False)
        return 'DataFrame saved to annotated_data.csv'
    return ''

if __name__ == '__main__':
    app.run_server(debug=True)

from typing import List
from fastapi import FastAPI, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
from PIL import Image
from pydantic import BaseModel
import json
from pandas import isnull

import os
app = FastAPI()

# Path to the React build folder
build_path = os.path.join(os.path.dirname(__file__), "build")

# Serve the static files from the React build folder
app.mount("/static", StaticFiles(directory=os.path.join(build_path, "static")), name="static")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIRECTORY = "./uploads"
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)

class PipelineInput(BaseModel):
    startDate: str
    endDate:str
    
@app.get("/{file_path:path}")
async def serve_static_files(file_path: str):
    full_path = os.path.join(build_path, file_path)

    # Check if the file exists and is a static file
    if os.path.exists(full_path) and os.path.isfile(full_path):
        return FileResponse(full_path)

    # Fallback to index.html only for non-API routes
    if not file_path.startswith("api/"):
        return FileResponse(os.path.join(build_path, "index.html"))

    # If no matching file or API route found, return 404
    return {"detail": "Not Found"}

@app.get("/")
async def serve_react_app():
    return FileResponse(os.path.join(build_path, "index.html"))

@app.post("/api/upload_csv")
async def upload_files(files: List[UploadFile] = File(...)):
    file_name_list = []
    for file in files:
        file_content = await file.read()
        print(file.filename)
        # Save the file to your desired location
        with open(f"uploads/{file.filename}", "wb") as f:
            file_name_list.append(file.filename)
            f.write(file_content)

    return {"status":"success","list": file_name_list}

@app.get("/api/getPipline")
def get_pipeline_data(startDate: str, endDate: str, dataFile: str, historyFile: str):

    start_date = datetime.strptime(startDate, '%Y-%m-%d')
    end_date = datetime.strptime(endDate, '%Y-%m-%d')

    file_path_opportunity = f"{UPLOAD_DIRECTORY}/{dataFile}"
    file_path_history = f"{UPLOAD_DIRECTORY}/{historyFile}"
    opportunity_data = pd.read_csv(file_path_opportunity)
    opportunity_history = pd.read_csv(file_path_history)

    # Parse dates and handle errors in conversion
    opportunity_data['Created Date'] = pd.to_datetime(opportunity_data['Created Date'], errors='coerce')
    opportunity_data['Close Date'] = pd.to_datetime(opportunity_data['Close Date'], errors='coerce')
    opportunity_history['Last Modified'] = pd.to_datetime(opportunity_history['Last Modified'], errors='coerce')
    opportunity_history['Close Date'] = pd.to_datetime(opportunity_history['Close Date'], errors='coerce')

    # Drop rows with un-parsable dates
    opportunity_data = opportunity_data.dropna(subset=['Created Date', 'Close Date'])
    opportunity_history = opportunity_history.dropna(subset=['Last Modified', 'Close Date'])

    # Ensure 'Net-New Dollars' is numeric
    opportunity_data['Net-New Dollars'] = pd.to_numeric(opportunity_data['Net-New Dollars'].replace('[\$,]', '', regex=True))

    str_field = ['Account Name', 'Created Date','Discovery Date','Close Date', 'Opportunity Name', 'Opportunity Owner', 'Stage','Lead Source','Opportunity Source','Type','Primary ERP','Lost Reason','SQL Quarter','Closed Quarter']
    num_field = ['Age','Net-New Dollars']

    for field in str_field:
        opportunity_data[field] = opportunity_data[field].fillna('')

    for field in num_field:
        opportunity_data[field] = opportunity_data[field].fillna(0)

    opportunity_data["Age"] = opportunity_data["Age"].astype('float')
    opportunity_data["Net-New Dollars"] = opportunity_data["Net-New Dollars"].astype('float')

    # Filter for opportunities created before the start date and of type "New Customer"
    pre_start_created_opps = opportunity_data[
        (opportunity_data['Created Date'] < start_date) & (opportunity_data['Type'] == 'New Customer')
    ]

    # Get latest status as of start date for each opportunity
    history_as_of_start = opportunity_history[opportunity_history['Last Modified'] <= start_date]
    history_as_of_start = history_as_of_start.sort_values(by=['Opportunity Name', 'Last Modified'], ascending=[True, False])
    latest_status_as_of_start = history_as_of_start.drop_duplicates('Opportunity Name', keep='first')

    # Identify open opportunities as of the start date
    open_opps_as_of_start = latest_status_as_of_start[
        (~latest_status_as_of_start['To Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]['Opportunity Name'].unique()

    # Filter for deals expected to close within the range [start_date, end_date] as of the start date
    closing_in_range_as_of_start = history_as_of_start[
        (history_as_of_start['Opportunity Name'].isin(open_opps_as_of_start)) &
        (history_as_of_start['Close Date'] >= start_date) &  # Ensure close date is on or after start_date
        (history_as_of_start['Close Date'] <= end_date)      # Ensure close date is on or before end_date
    ]['Opportunity Name'].unique()

    # Calculate beginning pipeline value, including only deals expected to close in the date range
    beginning_pipeline_opps = pre_start_created_opps[
        pre_start_created_opps['Opportunity Name'].isin(closing_in_range_as_of_start) &
        (pre_start_created_opps['Close Date'] >= start_date) &  # Ensure close date is on or after start_date
        (pre_start_created_opps['Close Date'] <= end_date)      # Ensure close date is on or before end_date
    ]
    beginning_pipeline_value = beginning_pipeline_opps['Net-New Dollars'].sum()

    # Step 2: Calculate New Pipeline within the date range
    new_pipeline_deals = opportunity_data[
        (opportunity_data['Created Date'] >= start_date) &
        (opportunity_data['Created Date'] <= end_date) &
        (opportunity_data['Close Date'] <= end_date) &
        (opportunity_data['Type'] == 'New Customer') &
        (opportunity_data['Stage'] != 'SQL - AE Accepted') &
        (~opportunity_data['Opportunity Name'].isin(opportunity_history[
            (opportunity_history['From Stage'] == 'SQL - AE Accepted') &
            (opportunity_history['To Stage'].isin(['Closed Won', 'Closed Lost', 'Closed Nurture']))
        ]['Opportunity Name'].unique()))
    ]
    new_pipeline_value = new_pipeline_deals['Net-New Dollars'].sum()

    # Step 3: Calculate 'Won' opportunities within the date range
    closed_won_new_customer_deals = opportunity_data[
        (opportunity_data['Stage'] == 'Closed Won') &
        (opportunity_data['Type'] == 'New Customer') &
        (opportunity_data['Close Date'] >= start_date) &
        (opportunity_data['Close Date'] <= end_date)
    ]
    won_value = closed_won_new_customer_deals['Net-New Dollars'].sum()

    # Step 4: Calculate 'Lost' opportunities within the date range, filtering out those previously in 'SQL - AE Accepted'
    closed_lost_nurture_history = opportunity_history[
        (opportunity_history['To Stage'].isin(['Closed Lost', 'Closed Nurture'])) &
        (opportunity_history['Last Modified'] >= start_date) &
        (opportunity_history['Last Modified'] <= end_date)
    ]
    sql_ae_prior_to_closed = opportunity_history[
        (opportunity_history['To Stage'].isin(['Closed Lost', 'Closed Nurture'])) &
        (opportunity_history['From Stage'] == 'SQL - AE Accepted')
    ]['Opportunity Name'].unique()
    filtered_transitions = closed_lost_nurture_history[
        ~closed_lost_nurture_history['Opportunity Name'].isin(sql_ae_prior_to_closed)
    ]
    filtered_transitions_unique = filtered_transitions.sort_values(by='Last Modified', ascending=False).drop_duplicates(subset=['Opportunity Name'])
    final_filtered_opps = filtered_transitions_unique.merge(
        opportunity_data[opportunity_data['Type'] == 'New Customer'],
        on="Opportunity Name",
        suffixes=('_history', '_pipeline')
    )
    closed_lost_value = final_filtered_opps['Net-New Dollars'].sum()

    ### Step 5: Calculate 'Pulled to Current Period' (Deals with close dates moved into the start and end date range)
    # Deals initially closing beyond end_date, then pulled to close within the period defined by start_date and end_date
    history_future = opportunity_history[
        (opportunity_history['Close Date'] > end_date) &
        (~opportunity_history['To Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]

    # Filter for records now closing within the start_date and end_date range
    history_within_period = opportunity_history[
        (opportunity_history['Close Date'] >= start_date) &
        (opportunity_history['Close Date'] <= end_date) &
        (~opportunity_history['To Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]

    # Identify deals pulled to the current period by merging future and within-period records
    pulled_to_current_period = history_future.merge(
        history_within_period,
        on="Opportunity Name",
        suffixes=('_future', '_current')
    )

    # Filter for "New Customer" type opportunities only and calculate the total Net-New Dollars for pulled deals
    pulled_to_current_period = pulled_to_current_period.merge(
        opportunity_data[(opportunity_data['Type'] == 'New Customer')][['Opportunity Name', 'Net-New Dollars']],
        on="Opportunity Name",
        how="inner"
    )
    pulled_to_2024_value = pulled_to_current_period['Net-New Dollars'].sum()

    # Define close_date_pulled_unique explicitly
    close_date_pulled_unique = pulled_to_current_period.drop_duplicates(subset=['Opportunity Name'])
    pulled_to_2024_value = close_date_pulled_unique['Net-New Dollars'].sum()

    ### Step 6: Calculate 'Pushed to Next Period' (Deals with close dates moved out beyond end_date)
    # Sort history to get the latest status for each opportunity
    latest_records = opportunity_history.sort_values(by=['Opportunity Name', 'Last Modified'], ascending=[True, False])
    latest_status = latest_records.drop_duplicates(subset=['Opportunity Name'], keep='first')

    # Filter for deals initially closing within the start_date and end_date range, then moved beyond end_date
    history_within_period = opportunity_history[
        (opportunity_history['Close Date'] >= start_date) &
        (opportunity_history['Close Date'] <= end_date) &
        (~opportunity_history['To Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]

    # Filter for records now pushed to close beyond end_date
    history_pushed_out = latest_status[
        (latest_status['Close Date'] > end_date) &
        (~latest_status['To Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]

    # Identify pushed deals by merging within-period and beyond-period records
    pushed_deals = history_within_period.merge(
        history_pushed_out[['Opportunity Name', 'Close Date', 'Last Modified']],
        on="Opportunity Name",
        suffixes=('_within', '_beyond')
    )

    # Filter for "New Customer" type opportunities only and calculate the total Net-New Dollars for pushed deals
    pushed_deals = pushed_deals.merge(
        opportunity_data[(opportunity_data['Type'] == 'New Customer')][['Opportunity Name', 'Net-New Dollars']],
        on="Opportunity Name",
        how="inner"
    )
    pushed_to_2025_value = pushed_deals['Net-New Dollars'].sum()

    # Define pushed_deals_unique explicitly
    pushed_deals_unique = pushed_deals.drop_duplicates(subset=['Opportunity Name'])
    pushed_to_2025_value = pushed_deals_unique['Net-New Dollars'].sum()

    # Step 7: Calculate the ending pipeline value based on dynamic dates
    ending_pipeline_opps = opportunity_data[
        (opportunity_data['Close Date'] <= end_date) &
        (opportunity_data['Type'] == 'New Customer') &
        (~opportunity_data['Stage'].isin(['Closed Lost', 'Closed Nurture', 'Closed Won', 'SQL - AE Accepted']))
    ]
    ending_pipeline_value_actual = ending_pipeline_opps['Net-New Dollars'].sum()

    # Calculate adjusted ending pipeline using cumulative adjustments
    calculated_ending_pipeline = beginning_pipeline_value + new_pipeline_value - won_value - closed_lost_value + pulled_to_2024_value - pushed_to_2025_value

    # Define values for bridge chart
    values = [
        beginning_pipeline_value / 1_000_000,
        (new_pipeline_value / 1_000_000) + (ending_pipeline_value_actual - calculated_ending_pipeline) / 1_000_000,
        -won_value / 1_000_000,
        -closed_lost_value / 1_000_000,
        pulled_to_2024_value / 1_000_000,
        -pushed_to_2025_value / 1_000_000,
        ending_pipeline_value_actual / 1_000_000
    ]
    categories = ['Beginning Pipeline', 'Adjusted New Pipeline', 'Won', 'Lost', 'Pulled to 2024', 'Pushed to 2025', 'Ending Pipeline']

    # Bridge chart setup
    cumulative_values = [0]
    for i in range(len(values) - 1):
        cumulative_values.append(cumulative_values[-1] + values[i])

    final_filtered_opps['Amount'] = final_filtered_opps['Amount'].fillna(0)
    final_filtered_opps['Expected Revenue'] = final_filtered_opps['Expected Revenue'].fillna(0)

    category_data = {
    "Beginning Pipeline": beginning_pipeline_opps,
    "Adjusted New Pipeline": new_pipeline_deals,
    "Won": closed_won_new_customer_deals,
    "Lost": final_filtered_opps,
    "Pulled to 2024": close_date_pulled_unique,
    "Pushed to 2025": pushed_deals_unique,
    "Ending Pipeline": ending_pipeline_opps
    }

    table_data = {}
    for key, value in category_data.items():   
        df = pd.DataFrame(value)
        table_data[key] = df.to_dict(orient="index")

    values = [round(num, 2) for num in values]

    return {"table_data":table_data, "values":values}

import os
import pandas as pd
from nselib import derivatives, capital_market
from datetime import datetime, timedelta

# Function to process data for a given date
def process_trade_date(trade_date):
    try:
        print(f"\nProcessing data for {trade_date}...")  

        # Fetch the F&O bhav copy data
        fno_data = derivatives.fno_bhav_copy(trade_date=trade_date)

        # List of columns you need for F&O data
        fno_required_columns = [
            'BizDt', 'TckrSymb', 'XpryDt', 'OpnIntrst',
            'ChngInOpnIntrst','FinInstrmNm', 'TtlTradgVol', 'HghPric',
            'LwPric', 'TtlTrfVal', 'NewBrdLotQty'
        ]

        # Filter the F&O data
        fno_filtered_data = fno_data[fno_required_columns].copy()

        # Rename columns for F&O data
        fno_filtered_data.rename(columns={
            'BizDt': 'Date',
            'TckrSymb': 'Symbol',
            'XpryDt': 'Expiry Date',
            'OpnIntrst': 'Open Interest',
            'ChngInOpnIntrst': 'Change in Open Interest',
            'FinInstrmNm': 'Instrument Name',
            'TtlTradgVol': 'Total Trading Volume',
            'HghPric': 'High Price',
            'LwPric': 'Low Price',
            'TtlTrfVal': 'Total Value',
            'NewBrdLotQty': 'Lot Size'
        }, inplace=True)

        # Filter for futures data only
        fno_filtered_data = fno_filtered_data[fno_filtered_data['Instrument Name'].str.endswith('FUT')]
        print("Filtered to include only Futures data.")

        # Load the CSV file that contains the stock symbols and their respective sectors
        sector_data = pd.read_csv('sectors.csv')

        # Merge the F&O data with sector data based on the 'Symbol' column
        updated_data = pd.merge(fno_filtered_data, sector_data[['Symbol', 'Sector']], on='Symbol', how='left')

        # Fetch the spot market data
        spot_data = capital_market.bhav_copy_with_delivery(trade_date=trade_date)

        # Filter the spot data
        spot_required_columns = ['SYMBOL', 'PREV_CLOSE', 'CLOSE_PRICE', 'DELIV_PER']
        spot_filtered_data = spot_data[spot_required_columns].copy()

        # Merge the spot data
        final_data = pd.merge(updated_data, spot_filtered_data,
                              left_on='Symbol', right_on='SYMBOL', how='left')

        # Drop redundant column
        final_data.drop(columns=['SYMBOL'], inplace=True)

        # Convert columns to appropriate data types
        numeric_columns = [
            'Open Interest', 'Change in Open Interest', 'Total Trading Volume',
            'High Price', 'Low Price','Total Value',
            'Lot Size', 'PREV_CLOSE', 'CLOSE_PRICE', 'DELIV_PER'
        ]
        for col in numeric_columns:
            final_data[col] = pd.to_numeric(final_data[col], errors='coerce')

        # Define aggregation functions
        aggregation_functions = {
            'Date': 'first',
            'Sector': 'first',
            'Open Interest': 'sum',
            'Change in Open Interest': 'sum',
            'CLOSE_PRICE': 'mean',
            'PREV_CLOSE': 'mean',
            'DELIV_PER': 'mean',
            'Instrument Name': 'first',
            'Total Trading Volume': 'sum',
            'High Price': 'max',
            'Low Price': 'min',
            'Total Value': 'sum',
            'Lot Size': 'first',
        }

        # Aggregate data to remove duplicate symbols
        aggregated_data = final_data.groupby('Symbol', as_index=False).agg(aggregation_functions)

        # Save the data to a file with date in the filename
        output_file = f"Masterdata_{trade_date.replace('-', '')}.csv"
        aggregated_data.to_csv(output_file, index=False)
        print(f"File saved successfully: {output_file}")

    except Exception as e:
        print(f"Holiday or Error processing date for {trade_date}: {e}")


# Function to process a range of dates
def process_date_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        trade_date = current_date.strftime('%d-%m-%Y')
        process_trade_date(trade_date)
        current_date += timedelta(days=1)


# Get the start and end date from environment variables
start_date_input = os.environ.get('START_DATE', '').strip()
end_date_input = os.environ.get('END_DATE', '').strip()

# Convert input dates to datetime objects
try:
    if start_date_input and end_date_input:
        start_date = datetime.strptime(start_date_input, '%d-%m-%Y')
        end_date = datetime.strptime(end_date_input, '%d-%m-%Y')

        if start_date > end_date:
            print("Start date cannot be after the end date.")
        else:
            process_date_range(start_date, end_date)
    else:
        print("Start and End date must be provided as environment variables.")

except ValueError:
    print("Invalid date format. Please use DD-MM-YYYY.")

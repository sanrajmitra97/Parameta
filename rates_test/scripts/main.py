import pandas as pd
import time
from typing import Union

pd.options.mode.chained_assignment = None  # default='warn'

COMMON_COLS = ['timestamp', 'security_id', 'price', 'ccy_pair', 'new_price']

class RatesProcessor:
    def __init__(self, ccy_df: pd.DataFrame, price_df: pd.DataFrame, spot_rate_df: pd.DataFrame) -> None:
        self.ccy_df = ccy_df.copy()
        self.price_df = price_df.copy()
        self.spot_rate_df = spot_rate_df.copy()

        self.required_conversions = None
    
    def set_required_conversions(self) -> None:
        required_conversions = {}
        ccy_dict = self.ccy_df[self.ccy_df['convert_price']].to_dict(orient='records')
        for element in ccy_dict:
            required_conversions[element['ccy_pair']] = element['conversion_factor']

        self.required_conversions = required_conversions
    
    def conversion_required(self, ccy_pair: str) -> bool:
        return ccy_pair in self.required_conversions

    def get_conversion_factor(self, ccy_pair: str) -> Union[float, None]:
        return self.required_conversions.get(ccy_pair, None)
    
    def run_pipeline(self) -> pd.DataFrame:
        # Find out which rows need conversion
        self.price_df['must_convert'] = self.price_df['ccy_pair'].apply(self.conversion_required)
        self.spot_rate_df['must_convert'] = self.spot_rate_df['ccy_pair'].apply(self.conversion_required)

        # Filter out those rows in spot_rate_df that don't require conversion
        self.spot_rate_df = self.spot_rate_df[self.spot_rate_df['must_convert']]

        # Convert the timestamp column to datetime
        self.spot_rate_df['timestamp'] = pd.to_datetime(self.spot_rate_df['timestamp'])
        self.price_df['timestamp'] = pd.to_datetime(self.price_df['timestamp'])

        # Distinguish between price rows that need new price calculation and those that don't
        conversion_price_df = self.price_df[self.price_df['must_convert']]
        no_conversion_price_df = self.price_df[~self.price_df['must_convert']]

        # Create a new 'new_price' column in no_conversion_price_df that is same as price
        no_conversion_price_df['new_price'] = no_conversion_price_df['price']

        # Get the conversion factor for each row in conversion_price_df
        conversion_price_df['conversion_factor'] = conversion_price_df['ccy_pair'].apply(self.get_conversion_factor)

        # Rename the conversion_price_df and spot_rate_df timestamp columns to avoid confusion during merge and debugging purposes
        conversion_price_df = conversion_price_df.rename(columns={'timestamp': 'timestamp_price'})
        self.spot_rate_df = self.spot_rate_df.rename(columns={'timestamp': 'timestamp_spot'})

        # Merge the two DataFrames on 'ccy_pair' and the one hour condition using merge_asof
        conversion_price_df = conversion_price_df.sort_values('timestamp_price')
        self.spot_rate_df = self.spot_rate_df.sort_values('timestamp_spot')
        merged_df = pd.merge_asof(conversion_price_df, 
                                self.spot_rate_df, 
                                left_on='timestamp_price', 
                                right_on='timestamp_spot', 
                                by='ccy_pair', 
                                direction='backward', 
                                tolerance=pd.Timedelta('1h'), 
                                suffixes=('_price', '_spot'))
                
        # We calculate the new price: (current price/ conversion factor) + spot_mid_rate
        merged_df['new_price'] = (merged_df['price'] / merged_df['conversion_factor']) + merged_df['spot_mid_rate']

        # If there are any NaN values in new_price, it means that the one hour window condition was not met
        
        # Finally, we concatenate the no_conversion_price_df and merged_df to get the final DataFrame
        merged_df = merged_df.rename(columns={'timestamp_price': 'timestamp'}) # Rename back for consistency
        merged_df = merged_df[COMMON_COLS] # Keep original columns + new_price
        no_conversion_price_df = no_conversion_price_df[COMMON_COLS]
        final_df = pd.concat([merged_df, no_conversion_price_df], ignore_index=True, axis=0)

        return final_df

def main() -> None:
    '''
    In this takehome, we don't require the user to provide the data as input.
    Ideally in a production scenario, we would query the data from a database.
    '''
    # Read in the data
    ccy_df = pd.read_csv('./rates_test/data/rates_ccy_data.csv')
    price_df = pd.read_parquet('./rates_test/data/rates_price_data.parq')
    spot_rate_df = pd.read_parquet('./rates_test/data/rates_spot_rate_data.parq')
    
    # Create RatesProcessor object
    rates_processor_obj = RatesProcessor(
        ccy_df=ccy_df,
        price_df=price_df,
        spot_rate_df=spot_rate_df
    )

    # Set required conversions
    rates_processor_obj.set_required_conversions()

    # Run the pipeline
    final_df = rates_processor_obj.run_pipeline()

    # Save the results as a csv
    final_df.to_csv('./rates_test/results/rates_final_data.csv', index=False)

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Took {time_taken:.3f} seconds to process rates data.")
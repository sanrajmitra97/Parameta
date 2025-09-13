import pandas as pd
import time
from typing import Tuple


class RollingStdevCalculator:
    def __init__(self, window_size: int = 20):
        self.window_size = window_size

    def pivot_and_reindex(self, df: pd.DataFrame, price_type: str, all_hours: pd.DatetimeIndex) -> pd.DataFrame:
        """
        Convert the dataframe from long to wide, and filling in all missing hourly snap times. 
        The new dataframe will have snap time as the index, security id's as the columns, and values would be the price type (i.e. bid, mid, or ask)

        Args:
            - df - DataFrame with columns ['snap_time', 'security_id', 'bid'/'mid'/'ask'].
            - price_type - A string that corresponds to either bid'/'mid'/'ask'.
            - all_hours - A range of time values spaced by one hour from the beginning to end_time.
        """
        df_wide = df.pivot(index='snap_time', columns='security_id', values=price_type)
        # Reindex the row index to include the full hourly range (so missing snaps become NaN). This also sorts the dataframe by the time since I know all_hours is increasing.
        df_wide = df_wide.reindex(all_hours)
        return df_wide

    def compute_rolling_stdev(self, df_wide: pd.DataFrame) -> pd.DataFrame:
        """
        For a wide dataframe where index = hourly snaps, columns = security ids,
        compute the rolling stdev for each column.
        
        So for each snap t, we return the most recent stdev computed from a
        contiguous window of size `self.window` that ends at some j <= t-1.

        If we enounter a missing stdvalue, we use ffill() to get the most recently computed valid std.

        Args:
            - df - The wide dataframe for a particular price type
        """
        # Rolling std. If we encounter NaN in the window of size 20, then stdev values are NaN. Only valid stdevs are considered.
        rolling_std_end = df_wide.rolling(window=self.window_size, min_periods=self.window_size).std() # 1 ddof since sample

        # We must use only windows that end at j <= t-1, so shift down by 1.
        # Then forward-fill the last found valid stdev so that at time t we use the
        # most recent available contiguous-window stdev.
        # If there was never a contiguous window before a given time, remains NaN
        stdev_at_snap = rolling_std_end.shift(1).ffill()
        
        return stdev_at_snap
    
    def unpivot(self, df_wide: pd.DataFrame, price_type: str) -> pd.DataFrame:
        """
        Unpivot a wide dataframe to a long dataframe using pandas melt.

        Args:
            - df_wide - The dataframe to conver.
            - price_type - Either bid, mid, or ask.
        """
        df_long = df_wide.reset_index(names=['snap_time']).melt(
            id_vars='snap_time',
            var_name='security_id',
            value_name=f'{price_type}_stdev'
        )
        return df_long     

    def run_pipeline(self,
                        original_df: pd.DataFrame,
                        start_time: str,
                        end_time: str,
                        output_path: str,
                        price_cols: Tuple[str] = ('bid', 'mid', 'ask')) -> None:
        """
        Main processing function. This function won't return anything, it will simply save the results to the results folder.

        Args:
            - original_df - DataFrame with columns ['snap_time', 'security_id', 'bid','mid','ask'].
            - start_time - Return rows from start_time -> end_time.
            - end_time - Return rows from start_time -> end_time.
            - output_path - Output Path to save results.
            - price_cols - which price columns to compute. Default ['bid', 'mid', 'ask'].

        Returns: result DataFrame in long format.
        """
        # Convert snap time to datetime
        df = original_df.copy()
        df['snap_time'] = pd.to_datetime(df['snap_time'])
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)

        # Filter dataset to be <= end_time to shorten compute time. We still need earlier rows to compute rolling stdev between start and end time.
        df = df[df['snap_time'] <= end_time]

        # Build the full hourly index starting at the earliest snap time in the available data so we can fill up the missing gaps in the hourly snap time.
        earliest_snap = df['snap_time'].min()
        all_hours = pd.date_range(start=earliest_snap, end=end_time, freq='h')

        stdev_results = {}
        for price in price_cols:
            # Now we convert, for each type of price, into a wide dataframe where each row is the snap time and each column is the security id from 1 -> last security id.
            # This will make it faster to compute the rolling stdev as it can be applied to all security id's at once.
            df_wide = self.pivot_and_reindex(df, price, all_hours)

            # Now we compute the stdevs
            stdev_wide = self.compute_rolling_stdev(df_wide)

            # Unpivot the dataframe
            stdev_long = self.unpivot(stdev_wide, price)

            # Filter the dataframe for only the window we want. Note, it has already been filtered for rows before the end time.
            stdev_long = stdev_long[stdev_long['snap_time'] >= start_time]

            # Store results
            stdev_results[price] = stdev_long

        # Finally, merge the three dataframes
        final_df = stdev_results['bid'].merge(stdev_results['mid'], on=['security_id', 'snap_time'], how='inner').merge(stdev_results['ask'], on=['security_id', 'snap_time'], how='inner')

        # Add back the original columns -> We are doing this to validate the results. If we don't need the original columns, we can skip this step.
        final_df = final_df.merge(original_df, on=['security_id', 'snap_time'], how='left')

        # Save CSV
        final_df.to_csv(output_path, index=False)


def main() -> None:
    '''
    Here, we specify the path to the data, and also the window size and start/end times. 
    If we had to run this script every hour, we can modify the start and end time variables every hour via a scheduler accordingly.
    '''
    
    DATA_PATH = './stdev_test/data/stdev_price_data.parq'
    OUTPUT_PATH = './stdev_test/results/stdev_test_output.csv'
    WINDOW_SIZE = 20
    START_TIME = '2021-11-20 00:00:00'
    END_TIME = '2021-11-23 09:00:00'

    df = pd.read_parquet(DATA_PATH)
    calc = RollingStdevCalculator(window_size=WINDOW_SIZE)
    calc.run_pipeline(df, START_TIME, END_TIME, OUTPUT_PATH)


if __name__ == "__main__":
    t0 = time.time()
    main()
    t1 = time.time()
    print(f"Computed stdevs in {t1 - t0:.3f}s")
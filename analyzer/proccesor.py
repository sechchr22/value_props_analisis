"""
    Data Proccesor Module
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from typing import Tuple
from analyzer.parser import Parser

class Processor():
    """
    Processor class for transforming and aggregating user interaction and payment data.
    """

    @staticmethod
    def get_dates_boundaries(df: pd.DataFrame) -> dict[str, pd.Timestamp]:
        """
        Extracts key date boundaries from the input DataFrame.

        Converts the 'day' column to datetime and computes:
        - most_recent_day: latest date in the dataset
        - one_week_ago: 7 days before the most recent date
        - three_weeks_ago: 21 days before the most recent date

        Args:
            df (pd.DataFrame): Input DataFrame with a 'day' column.

        Returns:
            dict[str, pd.Timestamp]: Dictionary of date boundaries.
        """
        df['day'] = pd.to_datetime(df['day'])
        most_recent_day = df['day'].max()
        return {
            'most_recent_day': most_recent_day,
            'one_week_ago': most_recent_day - pd.Timedelta(weeks=1),
            'three_weeks_ago': most_recent_day - pd.Timedelta(weeks=3)
        }

    @staticmethod
    def get_print_tap_count(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates print/tap events per user and value_prop within a 3-week window.

        Args:
            df (pd.DataFrame): Raw print/taps DataFrame.

        Returns:
            pd.DataFrame: Enriched DataFrame with event counts per user/value_prop.
        """
        dates = Processor.get_dates_boundaries(df)
        df = pd.json_normalize(df.to_dict(orient='records'))
        df.drop(columns='event_data.position', inplace=True)
        df.rename(columns={'event_data.value_prop': 'value_prop'}, inplace=True)
        agg_df = (
            df
            .loc[df['day'].between(dates['three_weeks_ago'], dates['most_recent_day'])]
            .groupby(['user_id', 'value_prop'])
            .size()
            .reset_index(name='count')
        )
        df = (
            df
            .merge(agg_df, on=['user_id', 'value_prop'], how='left')
            .fillna(0)
            .astype({'count': 'int'})
            .reset_index(drop=True)
        )
        return df
    
    @staticmethod
    def get_payments_info(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates payment activity per user and value_prop within a 3-week window.

        Computes:
        - payments: number of transactions
        - accumulated_amount: total value of transactions

        Args:
            df (pd.DataFrame): Raw Payments DataFrame.

        Returns:
            pd.DataFrame: Enriched DataFrame with payment metrics.
        """
        df.rename(columns={'pay_date': 'day'}, inplace=True)
        dates = Processor.get_dates_boundaries(df)
        agg_df = (
            df
            .loc[df['day'].between(dates['three_weeks_ago'], dates['most_recent_day'])]
            .groupby(['user_id', 'value_prop'])
            .agg(payments=('total', 'size'), accumulated_amount=('total', 'sum'))
            .reset_index()
        )
        df = (
            df
            .merge(agg_df, on=['user_id', 'value_prop'], how='left')
            .fillna(0)
            .drop(columns=['day', 'total'])
            .astype({'payments': 'int'})
            .reset_index(drop=True)
        )
        return df

    @staticmethod
    def dispatch(task_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Routes the input DataFrame to the appropriate processing function.

        Args:
            task_name (str): One of ['prints', 'taps', 'payments'].
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Processed DataFrame.

        Raises:
            ValueError: If task_name is not recognized.
        """
        if task_name == 'prints' or task_name == 'taps':
            return Processor.get_print_tap_count(df)
        elif task_name == 'payments':
            return Processor.get_payments_info(df)
        else:
            raise ValueError(f"Unknown task: {task_name}")

    @staticmethod
    def compute_dfs(files: list[str]) -> Tuple[list[pd.DataFrame], dict[pd.Timestamp]]:
        """
        Parses (concurrently) and processes (in parallel) multiple input files.

        Args:
            files (list[str]): List of file paths.

        Returns:
            Tuple[list[pd.DataFrame], dict[pd.Timestamp]]:
                - List of processed DataFrames [prints, taps, payments]
                - Date boundaries dictionary
        """
        with ThreadPoolExecutor() as executor:
            prints_df, taps_df, pays_df = list(executor.map(Parser.parse, files))
        dates = Processor.get_dates_boundaries(prints_df)
        
        tasks = [
            ('prints', prints_df),
            ('taps', taps_df),
            ('payments', pays_df)
        ]
        with Pool(processes=len(tasks)) as pool:
            results = pool.starmap(Processor.dispatch, tasks)
            
        return results, dates

    @staticmethod
    def get_final_df(files: list[str]) -> pd.DataFrame:
        """
        Produces the final merged DataFrame combining prints, taps, and payments.

        Args:
            files (list[str]): List of file paths.

        Returns:
            pd.DataFrame: Final enriched and filtered DataFrame.
        """
        cumputed_dfs, dates = Processor.compute_dfs(files)
        prints, taps, payments, = cumputed_dfs

        prints.rename(columns={'count': 'prints'}, inplace=True)
        taps = (taps.rename(columns={'count': 'taps'}).drop(columns='day'))

        final_df = (
            prints
            .merge(taps, on=['user_id', 'value_prop'], how='left')
            .fillna({'prints': 0, 'taps': 0})
            .astype({'prints': 'int', 'taps': 'int'})
            .assign(clicked=lambda df: np.where(df['taps'] == 0, 'no', 'yes'))
            .merge(payments, on=['user_id', 'value_prop'], how='left')
            .fillna(0)
            .astype({'payments': 'int'})
            .loc[lambda d: d['day'].between(dates['one_week_ago'], dates['most_recent_day'])]
            .drop(columns='day')
            .sort_values(by=['user_id', 'accumulated_amount'], ascending=[True, False])
            .reset_index(drop=True)
            .drop_duplicates()
        )
        return final_df
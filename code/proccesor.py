"""
    Data Proccesor Module
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from typing import Tuple
from value_props_analisis.parser import Parser

class Processor():
    """
        DataProcessor Class
    """
    @staticmethod
    def get_dates_boundaries(df: pd.DataFrame) -> dict[str, pd.Timestamp]:
        # Getting most_recent_day, one_week_ago and three_weeks_ago
        df['day'] = pd.to_datetime(df['day'])
        most_recent_day = df['day'].max()
        return {
            'most_recent_day': most_recent_day,
            'one_week_ago': most_recent_day - pd.Timedelta(weeks=1),
            'three_weeks_ago': most_recent_day - pd.Timedelta(weeks=3)
        }

    @staticmethod
    def get_print_tap_count(df: pd.DataFrame) -> pd.DataFrame:
        # Get Dates Boundaries
        dates = Processor.get_dates_boundaries(df)
        # normalize nested fields to get value props from prints and taps
        df = pd.json_normalize(df.to_dict(orient='records'))
        df.drop(columns='event_data.position', inplace=True)
        df.rename(columns={'event_data.value_prop': 'value_prop'}, inplace=True)
        # Pipeline
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
        # Get Dates Boundaries
        df.rename(columns={'pay_date': 'day'}, inplace=True)
        dates = Processor.get_dates_boundaries(df)
        # Pipeline
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
        if task_name == 'prints' or task_name == 'taps':
            return Processor.get_print_tap_count(df)
        elif task_name == 'payments':
            return Processor.get_payments_info(df)
        else:
            raise ValueError(f"Unknown task: {task_name}")

    @staticmethod
    def compute_dfs(files: list[str]) -> Tuple[list[pd.DataFrame], dict[pd.Timestamp]]:
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
        cumputed_dfs, dates = Processor.compute_dfs(files)
        prints, taps, payments, = cumputed_dfs

        prints.rename(columns={'count': 'prints'}, inplace=True)
        taps = (taps.rename(columns={'count': 'taps'}).drop(columns='day'))

        merged_df = (
            prints
            .merge(taps, on=['user_id', 'value_prop'], how='left')
            .fillna({'prints': 0, 'taps': 0})
            .astype({'prints': 'int', 'taps': 'int'})
        )
        merged_df['clicked'] = np.where(merged_df['taps'] == 0, 'no', 'yes')

        return (
            merged_df
            .merge(payments, on=['user_id', 'value_prop'], how='left')
            .loc[lambda d: d['day'].between(dates['one_week_ago'], dates['most_recent_day'])]
            .fillna(0)
            .astype({'payments': 'int'})
            .sort_values(by=['user_id', 'value_prop', 'accumulated_amount'], ascending=[True, True, False])
            .reset_index(drop=True)
            .drop(columns='day')
            .drop_duplicates()
        )
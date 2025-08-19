import pandas as pd

class Cleaner():
    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates()
        df = df.dropna()
        df.columns = df.columns.str.lower()
        return df
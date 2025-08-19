import pandas as pd

class Cleaner():
    """
    Cleaner class for basic DataFrame hygiene operations.
    """

    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the input DataFrame by applying standard preprocessing steps.

        Args:
            df (pd.DataFrame): Raw DataFrame to be cleaned.

        Returns:
            pd.DataFrame: Cleaned and normalized DataFrame.
        """
        df = df.drop_duplicates()
        df = df.dropna()
        df.columns = df.columns.str.lower()
        return df
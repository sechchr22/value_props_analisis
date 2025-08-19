import pandas as pd
class Parser:
    """
    Parser class for ingesting structured data files.
    """

    @staticmethod
    def parse(file_path: str) -> pd.DataFrame:
        """
        Parses a file into a pandas DataFrame based on its format.

        Supported formats:
        - '.json': Treated as NDJSON (newline-delimited JSON)
        - '.csv': Standard comma-separated values

        Args:
            file_path (str): Path to the input file.

        Returns:
            pd.DataFrame: Parsed DataFrame.

        Raises:
            Exception: If the file format is unsupported.
        """
        if '.json' in file_path:
            return pd.read_json(file_path, lines=True)
        elif '.csv' in file_path:
            return pd.read_csv(file_path)
        else:
            raise Exception("Not valid file format. Valid Formats [CSV, NDJSON]")
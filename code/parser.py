import pandas as pd

class Parser:
    @staticmethod
    def parse(file_path: str) -> pd.DataFrame:
        if '.json' in file_path:
            return pd.read_json(file_path, lines=True)
        elif '.csv' in file_path:
            return pd.read_csv(file_path)
        else:
            raise Exception("Not valid file format. Valid Formats [CSV, NDJSON]")
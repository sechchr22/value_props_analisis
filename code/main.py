import os
import pdb
from value_props_analisis.proccesor import Processor

if __name__ == '__main__':
    BASE_DIR = os.path.dirname(__file__)
    file_paths = [
        os.path.join(BASE_DIR, 'data/prints.json'),
        os.path.join(BASE_DIR, 'data/taps.json'),
        os.path.join(BASE_DIR, 'data/pays.csv')
    ]
    final_df = Processor.get_final_df(file_paths)
    pdb.set_trace()
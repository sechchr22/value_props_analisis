import os
from analyzer.proccesor import Processor

if __name__ == '__main__':
    BASE_DIR = os.path.dirname(__file__)
    file_paths = [
        os.path.join(BASE_DIR, 'data/prints.json'),
        os.path.join(BASE_DIR, 'data/taps.json'),
        os.path.join(BASE_DIR, 'data/pays.csv')
    ]
    final_df = Processor.get_final_df(file_paths)

    output_path = os.path.join(BASE_DIR, 'data/final_dataset.csv')
    final_df.to_csv(output_path, index=False)
    print(f"✅ Final dataset saved to: {output_path}")

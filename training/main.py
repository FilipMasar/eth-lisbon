import pandas as pd
import glob, sys

import argparse

def local_training(input_dir, output_dir):
    print("Loading Dataset")
    data = glob.glob(f"{input_dir}/*csv")[0]
    df = pd.read_csv(data, header=None)

    avg = df.iloc[: , -1].mean()
    len = df.shape[0]

    print("Average:", avg)
    print("Lenght:", len)

    print("Saving Model")
    with open(f'{output_dir}/model.txt', 'w') as f:
        f.write(str(avg) + '\n')
        f.write(str(len) + '\n')


def aggregation(input_dir, output_dir):
    print("TODO")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Description of your program')
    
    parser.add_argument('--train', action='store_true', help='Train the model')
    parser.add_argument('--aggregate', action='store_true', help='Aggregate the results')
    parser.add_argument('--input', type=str, help='Input directory name', required=True)
    parser.add_argument('--output', type=str, help='Output file name', required=True)

    args = parser.parse_args()

    if args.train:
        # Train the model
        local_training(args.input, args.output)
    elif args.aggregate:
        # Aggregate the results
        aggregation(args.input, args.output)
    else:
        print("Must pass either --train or --aggregate")
        sys.exit()


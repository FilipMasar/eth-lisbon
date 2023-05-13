import pandas as pd
import glob, os, sys


def main(input_dir, output_dir):
    data = glob.glob(f"{input_dir}/*csv")[0]

    print("Loading Dataset")
    df = pd.read_csv(data, header=None)

    avg = df.iloc[: , -1].mean()
    len = df.shape[0]

    print(avg)
    print(len)

    print("Saving Model")
    with open(f'{output_dir}/model.txt', 'w') as f:
        f.write(str(avg) + '\n')
        f.write(str(len) + '\n')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Must pass arguments. Format: [command] input_dir output_dir")
        sys.exit()
    main(sys.argv[1], sys.argv[2])

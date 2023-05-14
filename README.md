# Federated Learning on Filecoin

## Quickstart

### Prerequisites
- Python 3.9
- [bacalhau cli](https://docs.bacalhau.org/getting-started/installation)

### Start the training
```bash
python3 workflow.py
```

### Datasets
Datasets for demontration are stored in `datasets` directory. They were published using [web3.storage](https://web3.storage/):
- bafybeiamyacldmsqo2plk4quzxmckqpcpckttt2asbj67jcssdlszlfr3a
- bafybeid3i7g5nqetotoj45xh32qjx2lrispstgjplbtcuedwhlvpzsly7i

If you want to use different datasets just update `hashes_train.txt` file with any number of cid's of your csv datasets.

## Overview

We provide two things in this repo:
- algorithms that allow doing federated learning
- a workflow which runs federated learning on Filecoin 

### Algorithms
There are two algorithms:
- local training - trains a model on a local dataset
- aggregation - aggregates the models trained on the local datasets

They are build as one docker image which is then used to run with the bacalhau cli.

Currently the algorithms do just one specific thing - compute average of last column of a csv file. However this can be extended to support machine learning models.

## What is Federated Learning?
Federated Learning is a machine learning technique that trains an algorithm across multiple decentralized edge devices or servers holding local data samples, without exchanging them. This approach stands in contrast to traditional centralized machine learning techniques where all the local datasets are uploaded to one server.


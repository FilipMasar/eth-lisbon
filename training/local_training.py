import csv

dataset = '/part1.csv'

# Open the CSV file
with open(dataset, newline='') as csvfile:
    # Create a CSV reader object
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')

    # Initialize an empty list to store the data
    data = []

    # Iterate over the rows in the CSV file
    for row in reader:
        # Append the row to the data list
        data.append(row)
    
# compute average of last column
last_column = [float(row[-1]) for row in data]
average = sum(last_column) / len(data)

# write to file average and number of rows
with open('model.txt', 'w') as f:
    f.write(str(average) + '\n')
    f.write(str(len(data)) + '\n')


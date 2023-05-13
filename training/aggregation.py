import sys

files = ['model.txt', 'model2.txt']

# loop through files and compute overall average
avg = 0
len = 0
for file in files:
  with open(file, 'r') as f:
    data = f.readlines()
    avg += float(data[0]) * int(data[1])
    len += int(data[1])

avg /= len

print("Overall average: " + str(avg))

    
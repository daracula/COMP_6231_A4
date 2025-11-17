from pyspark import SparkContext

sc = SparkContext("local[*]")
txt = sc.textFile('receipt.txt')

# Given receipt.txt containing items and their prices, calculate the total revenue of the store.
# lemon 48.50

items_mapped = txt.map(lambda x: (x.split()[0], float(x.split()[1])))
# print(items_mapped.take(10))

# total_costs = items_mapped.reduceByKey(lambda x, y: x + y)
# for item, cost in total_costs.collect():
    # print(f"{item}, cost: {cost}")

total = items_mapped.values().reduce(lambda x, y: x + y)

print(total)


scs = sc.textFile('students.txt')
# 4013111 COMP102 SOEN2042

courses_students = scs.flatMap(lambda x: [(c, 1) for c in x.split()[1:]])
courses_counts = courses_students.reduceByKey(lambda x, y: x + y)

print(courses_counts.collect())
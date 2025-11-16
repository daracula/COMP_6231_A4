from pyspark import SparkContext
sc = SparkContext("local[*]")

# read the text file
lines = sc.textFile('shakespear.txt')
# map each lines to word occurrences
lines_mapped = lines.flatMap(lambda x: [(word, 1) for word in x.split()])
# reduce each word tuples by summing the occurrences
counts = lines_mapped.reduceByKey(lambda x, y: x+y)
counts_sorted = counts.sortBy(lambda x: x[1], ascending=False)

print(counts_sorted.collect())

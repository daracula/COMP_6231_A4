from pyspark import SparkContext
sc = SparkContext("local[*]")

# read the text file
words = sc.textFile('grocery.txt')
# map each word with one occurrence
words_mapped = words.map(lambda x: (x, 1))
# reduce each word tuples by summing the occurrences
counts = words_mapped.reduceByKey(lambda x, y: x+y)

print(counts.collect())

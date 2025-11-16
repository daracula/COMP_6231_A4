# import os
# os.environ['JAVA_HOME'] = '/mnt/c/Users/dcunni5/.jdks/ms-21.0.8'
# os.environ['SPARK_HOME'] = '/mnt/c/Users/dcunni5/spark/spark-4.0.1-bin-hadoop3'

import pyspark
sc = pyspark.SparkContext('local[*]')

txt = sc.textFile('sample.log')

error_lines = txt.filter(lambda line: 'error' in line)

print(error_lines.count())


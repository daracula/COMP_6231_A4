def isprime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if not n & 1:
        return False
    for x in range(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True

from pyspark import SparkContext
sc = SparkContext("local[*]")
nums = sc.parallelize(range(10000000))
num_primes = nums.map(lambda x: isprime(x))
primes = num_primes.filter(lambda x: x)
print(primes.count())
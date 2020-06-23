import re
from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster('local').setAppName("Sorted_count_words")
sc = SparkContext(conf = conf)

def callmet(j):
	return re.compile(r'\W+', re.UNICODE).split(j.lower())

rdd = sc.textFile("Book.txt")
temp = rdd.flatMap(callmet)

words = temp.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey().collect()

for i in words:
	count = str(i[0])
	word = str(i[1])
	print(word+':\t'+count)

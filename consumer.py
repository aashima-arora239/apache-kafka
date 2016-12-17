from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from operator import add

if __name__ == "__main__":

    sc = SparkContext(appName="TwitterWordCount")
    ssc = StreamingContext(sc, 5)
    word_count = dict()
    
    directKafkaStream = KafkaUtils.createDirectStream(ssc, ["Tweets"], {"metadata.broker.list": "localhost:9092"})
    def wordrdd(x):
        print "wordrdd called"
        string_array = x[0][1].split(" ")
        for word in string_array:
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1

        print_dict(word_count)

    def print_dict(word_count):
        print("Word Counts")
        for idx, x in enumerate(word_count):
            word = x
            count = word_count[x]
            print(str(word) + " = " + str(count))

    def process(time, rdd):
        print "process called"
        try:
            data = rdd.map(lambda x: (x, 1)).reduceByKey(add)
            data.foreach(wordrdd)
        except:
            print('Error occured')

    directKafkaStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

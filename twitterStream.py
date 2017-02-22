from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    plt.plot(counts[1],counts[0],label="positive")
    plt.plot(counts[3],counts[2],label="negative")
    plt.xlabel("Time step")
    plt.ylabel("Word Count")
    plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=2, mode="expand", borderaxespad=0.)

    plt.show()



def load_wordlist(filename):
    file = open(filename,'r')
    wordSet=set()
    for word in file:
        wordSet.add(word[:-1])
    return wordSet


def checkWord(word,pword,nword):
    if word in pword:
        return ("positive",1)
    elif word in nword:
        return ("negative",1)
    else:
        return ("filler",1)

def getCounts(currentRDD,counts):
    current=currentRDD.collect()
    counts[0].append(current[0][1])
    counts[1].append(len(counts[0])-1)
    counts[2].append(current[1][1])
    counts[3].append(len(counts[2])-1)

def updateFunction(newValue,runningCount):
    return sum(newValue)+(runningCount or 0)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    counts = []
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    #tweets.foreachRDD(lambda x:TweetIterator(x,pwords,nwords,counts))
    totalCount=tweets.flatMap(lambda x:x.split()).map(lambda x:checkWord(x,pwords,nwords))
    totalCount=totalCount.filter(lambda x:str(x[0])!='filler').reduceByKey(lambda x,y:x+y)
    runningCount=totalCount.updateStateByKey(updateFunction)
    runningCount.pprint()
    counts.append([])
    counts.append([])
    counts.append([])
    counts.append([])
    totalCount.foreachRDD(lambda x:getCounts(x,counts))
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)

    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()


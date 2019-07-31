from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("SPark")
         .set("spark.executor.memory", "4g"))
sc = SparkContext(conf = conf)

logs = sc.textFile('C:/Codes/NasaLogs/*')
logs = logs.cache()


#count unique hostnames
cdis = logs.map(lambda line: line.split(' ')[0]).distinct().count()
print("Total unique hostnames: " + str(cdis))


#validate 404 index value
def err404(line):
    try:
       request = line.split(' ')[-2]
       if request == '404':
            return True
    except:
        pass
    return False

#count 404 errors
cerror = logs.filter(err404).cache()
print("Total 404 errors: " + str(cerror.count()))


#top hostname errors
def errhostname(dflogs):
    hostname = dflogs.map(lambda line: line.split(' ')[0])
    errcount = hostname.map(lambda hostname: (hostname, 1)).reduceByKey(add)
    top = errcount.sortBy(lambda pair: -pair[1]).take(5)
    
    for hostname, count in top:
        print(hostname, count)
        
    return top
	
errhostname(cerror)

#error by day
def errorbyday(dferror):
	days = dferror.map(lambda line: line.split('[')[1].split(':')[0])
	counts = days.map(lambda day: (day, 1)).reduceByKey(add).collect()
    
	for day, count in counts:
		print(day, count)

	return counts

errorbyday(cerror)


#total bytes
def sumbytes(dflogs):
    def countbytes(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0
        
    count = dflogs.map(countbytes).reduce(add)
    return count

print("Total bytes: " + str(sumbytes(logs)))

sc.stop()
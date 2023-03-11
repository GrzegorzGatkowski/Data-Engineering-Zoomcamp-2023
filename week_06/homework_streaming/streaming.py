from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName('PopularPULocation').getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)

kafkaParams = {"bootstrap.servers": "localhost:9092"}

kafka_stream_fhv = KafkaUtils.createDirectStream(ssc, ['rides_fhv'], kafkaParams)
kafka_stream_green = KafkaUtils.createDirectStream(ssc, ['rides_green'], kafkaParams)

kafka_stream_all = kafka_stream_fhv.union(kafka_stream_green)
kafka_stream_all.map(lambda x: x[1]).foreachRDD(lambda rdd: rdd.foreachPartition(process_partition))

def process_partition(iter):
    for record in iter:
        record = record.split(',')
        pu_location_id = record[6]
        yield pu_location_id

    popular_pu_locations = kafka_stream_all.flatMap(lambda x: x[1].split(',')) \
        .filter(lambda x: len(x) > 0) \
        .map(lambda x: (x.split(',')[6], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))

    popular_pu_locations.foreachRDD(lambda rdd: rdd.toDF(['PU_Location_ID', 'Count']).orderBy(desc('Count')).show())

ssc.start()
ssc.awaitTermination()

%spark.pyspark

df = spark.read.json("/tmp/data/part-00000-71106411-f8ad-4764-886f-6e616d007fd2-c000.json")

df.head(25)

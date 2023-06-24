# Read Order Data from hdfs and apply the business logic.

1. ProcessOrderData.scala for complete code implementation.
2. CommandHelper.scala generic method for reading the command line arguments.
3. Move the code to appropriate path on edge node and run it using below spark submit

spark-submit --class com.spark.assignment.ProcessOrderData --master yarn --deploy-mode cluster --queue dev_queue --executor-memory 4G --executor-cores 3 --driver-memory 2G --conf spark.shuffle.service.enabled=true
--conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.yarn.maxAppAttempts=1 /home/sankeerthan/sparkassignment/spark_assignment.jar -inputPath /user/sankeerthan/data.csv

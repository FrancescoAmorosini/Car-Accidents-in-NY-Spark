You can either run the examples in local mode or on a cluster

### Start a Spark cluster

Download spark (compiled version) and from the main folder run the following commands
- Start a master: ./sbin/start-master.sh
- Start a slave (executor): ./sbin/start-slave.sh

In the case you want to record events on the history server
- Add "spark.eventLog.enabled true" in the file conf/spark-defaults.conf (before starting the master)
- Add "spark.eventLog.dir /path/to/your/log/dir" in the file conf/spark-defaults.conf (before starting the master)
- Start a history server: ./sbin/start-history-server.sh

### Compile the project and obtain a jar

From the main folder of the project (where the pom.xml file is located)
- mvn package

### Submit a job

From the Spark main folder
- ./bin/spark-submit --class main.class.you.want.to.run /path/to/generated/jar <args>
- ./bin/spark-submit --class it.polimi.middleware.spark.car.accidents.CarAccidents ./target/spark_tutorial-1.jar
- spark-submit --class it.polimi.middleware.spark.car.accidents.CarAccidents ./target/car_accidents_spark-1.jar --master spark://3.236.20.171:7077
### Check the execution

From you browser
- 127.0.0.1:8080
- 127.0.0.1:18080 (history server)

#### Note
The same project has also been developed using [OpenMPI](https://github.com/simonestaffa/mw-open-mpi).

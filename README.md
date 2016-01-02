# junobeach

Playground for some data lake projects and POCs

## loggerhead
=======

Spark-Streaming playground.

* Kafka topic
* Producing XML events
* Spark Ingest
* Join with metadata
* Creating windowed functions/alerts
* Hive Output

### XML Data Generator

Generates addresses in XML format and pushes them to a given kafka broker and topic.
The rate is controllable in terms of min_delay and max_delay in ms.
You can also limit the total number of messages sent to kafka.

A version with multiple parallel producers is planned.


### Spark (Scala) - loggerhead

Please see loggerhead project in scala folder.

### PySpark

Coming soon.



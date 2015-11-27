# loggerhead

Spark streaming ingests and transformations.
Ingests data from a Kafka topic, 
joins with data from Hive (could be any JDBC/ODBC database),
uses a windowing function to create alerts for particular conditions,
and exports the processed events to Hive.

![Loggerhead Turtle](https://dl.dropboxusercontent.com/u/4178609/Loggerhead-sea-turtle.png "Loggerhead Sea Turtle")

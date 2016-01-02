# loggerhead

![Loggerhead Turtle](https://dl.dropboxusercontent.com/u/4178609/Loggerhead-sea-turtle.png "Loggerhead Sea Turtle")

## Summary

Spark streaming ingests and transformations.

* Ingests data from a Kafka topic, 
* joins with data from Hive (could be any JDBC/ODBC database),
* uses a windowing function to create alerts for particular conditions,
* exports alerts as json
* exports the processed events to Hive.

# Quickstart

Assuming you're using the HWX sandbox -- adapt to other envs as needed.

Go back out to the address-generator and run:
    python generate_addresses.py -k sandbox.hortonworks.com:6667 -t testing

To run the spark streaming job, edit scripts/submit-job.sh to match your env and run:
    sbt assembly
    ./scripts/submit-job.sh

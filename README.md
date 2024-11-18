
Modified from the version in master branch as follows:

- Added Kafka dependency
- Removed pipeline step to perform vector search
-   this in turn changed type of pipeline item from ProcessedPayment to just Payment
- Added pipeline sink to Kafka
- Used 'toString' method in Payment since we don't have a serializer for Payment type when writing to the Sink. 

To run, I'm starting up Zookeeper and Kafka on my local system, no special configuration.




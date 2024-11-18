
Thiis is imported from Tomasz' blog (https://hazelcast.com/blog/hazelcast-platform-and-cdc-a-perfect-pair-for-your-system/)

A few changes were made to the POM:
- added hazelcast_enterprise_repo repository
- changed Hazelcast version from 6.0.0 to 5.5.2 
- changed Java version from 23 to 21

In order to run it needs a license key that has ADVANCED_AI feature (among others), rather than add to the project I set if via env var HZ_LICENSEKEY

For my purposes I don't need the vector search / AI features but wanted to add writing to Kafka, rather than modify the main branch I made these changes in 'kafka' branch.


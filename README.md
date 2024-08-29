
<h2>This Repo Contains 2 Utility Applications</h2>


<b>Redis Data Loader</b>

This app uses Jedis (Jedis Pipelining) to load data from a CSV file into Redis Hash or JSON objects.

It can add 1 level of nesting in JSON, to support 1 to Many use cases. e.g. Order Header > Order Line Items

See sample file Subscriptions-10.csv

Note: This code is for demo / learning purposes only. Use the RIOT utility to load data into Redis.

To run the application.

#1 Download the repository from GIT (this uses maven as the build tool)

#2 Create config.properties with connection details (use sample-config.properties as the template)

redis.host=localhost
redis.port=6379
redis.user=default
redis.password=<password>
data.file=./Subscriptions-10.csv.csv
data.record.limit=0
data.key.prefix=subscriptions:
data.header.field=SubscriberID
data.detail.field=DependantUniqueID
data.detail.attr.name=dependants

#3 Edit the App.java specify the path the csv file you wish to load.

#4 Execute App.java

This app uses Jedis Pipelining to load data from a CSV file into Redis Hash an or JSON objects.
It can add 1 level of nesting in JSON, to support 1 to Many use cases.
e.g. Order Header > Order Line Items

See sample file Sunscriptions-10.csv

Redis connection details are loaded from a file called config.properties. (Use sample-config.properties as a template.)

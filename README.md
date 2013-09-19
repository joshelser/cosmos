Cosmos [![Build Status](https://travis-ci.org/joshelser/cosmos.png)](https://travis-ci.org/joshelser/cosmos)
========

Cosmos was driven by the necessity to sort, filter and count large swaths of 
data queried from an Accumulo instance. Cosmos currently accepts columnar data sets
without a guaranteed schema. Because of this, and the support for multiple users 
to write concurrently to one Accumulo instance using Cosmos, this software scales 
well given sufficient Accumulo nodes for the concurrent client write threads.

## Using Cosmos

Build the cosmos-core jar using `mvn package` and place it on the Accumulo classpath
in addition to your application. Your client code must then implement the transformation
from your data model to the MultimapQueryResult. From here, you can treat your records 
uniformly using the Cosmos API.


## Functionality

Cosmos supports the followign features:

1. Fetch records by unique identifier
2. Fetch records by column, ascending or descending
3. Fetch records by value in a column
4. Count unique occurrences of a value in a column (SQL groupby)

For each call above, result sets can also be paginated by Cosmos to alleviate large result sets. 
As such, each operation lazily creates the result set so as to avoid excessive Java heap usage.

## Requirements

Cosmos requires a functioning Accumulo instance and a ZooKeeper instance (can be the same as the 
ZooKeeper instance used by the Accumulo installation).

## License

In keeping with the Apache Hadoop ecosphere, Cosmos is licensed with the Apache Software License.

Copyright 2013 Josh Elser

## Contributors

Josh Elser -- SRA International

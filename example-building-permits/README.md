A basic example of how Cosmos can be used, enumerating the different capabilities of Cosmos. All commands assume that they are executed from the directory in which this README is located unless otherwise noted.

## Data set
This example uses a collection of building permits from the city of Chicago, Illinois published at https://data.cityofchicago.org/.

The data can be found at https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu, and downloaded as CSV from https://data.cityofchicago.org/api/views/ydr8-5enu/rows.csv?accessType=DOWNLOAD. Alternatively, there is a script, download.sh, which will download it into src/main/resources, which is the default location the code looks for the dataset.

    $ ./download.sh

## Running the example

First, install Cosmos into your local Maven repository so we can work in a sub-module and still have access to every module's artifact in the project. This should be invoked from the root directory of the repository

    $ mvn install

If you're impatient, you can also skip the tests

    $ mvn install -DskipTests

### Using MiniAccumuloCluster

If using the MiniAccumuloCluster, you can simply invoke the following command

    $ mvn exec:exec -Prun-mac

This will create a directory in your system's tmpfs, so make sure you have adequate space if loading the entire dataset.

### Using a standalone Accumulo instance

You can provide parameters for the Accumulo instance name, a comma-separated list of zookeepers, and the Accumulo user to use with the accompanying password.

    $ mvn exec:exec -Prun -Dzookeepers=localhost:2181 -DinstanceName=accumulo -Dusername=root -Dpassword=password

The options provided in the above command are the defaults, so you may omit any that happen to match your local instance.

## So, what next

After loading (a subset of) the results from the provided file, the code will run through columns (ignoring the "ID" column and columns that begin with "CONTRACTOR_"), call groupBy on each column, and calculate the top-ten most frequent values for each column.

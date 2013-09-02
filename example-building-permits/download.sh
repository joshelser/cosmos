#!/bin/sh

curl 'https://data.cityofchicago.org/api/views/ydr8-5enu/rows.csv?accessType=DOWNLOAD' > src/main/resources/Building_Permits.csv

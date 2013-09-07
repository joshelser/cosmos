#!/bin/sh

mkdir -p src/main/resources
curl 'https://data.cityofchicago.org/api/views/ydr8-5enu/rows.csv?accessType=DOWNLOAD' > src/main/resources/Building_Permits.csv

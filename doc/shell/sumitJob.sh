#!/usr/bin/env bash

# --sql demo.sql                              special sql file demo.sql
# --state.backend rocksdb                     add properties state.backend as rocksdb
# --job.prop.file demoJobPropFile.properties  special job properties
# parameter priority : special parameter is hightest, next is job.prop.file, default properties [sqlSubmit.properties] last
#sh sqlSubmit.sh --sql demo.sql --state.backend rocksdb --job.prop.file demoJobPropFile.properties





flink run -yd -yid $session_status -c com.rookie.submit.main.SqlSubmit original-sqlSubmit-0.1.jar $@


$FLINK_DIR/bin/flink run -d -p 4 ../target/flink-sql-submit.jar -w "${PROJECT_DIR}"/src/main/resources/sql/dml/ -f "$1" -jn "$2"



$FLINK_HOME/bin/flink run  -c com.rookie.submit.main.SqlSubmit sqlSubmit-0.1.jar --sql q1.sql --state.backend rocksdb --job.prop.file sqlSubmit.properties
./bin/flatc-mac-22.9.24/flatc-mac  --python -o "tablestore/flatbuffer/dataprotocol"  tablestore/flatbuffer/sql.fbs
mv tablestore/flatbuffer/dataprotocol/tablestore/flatbuffer/dataprotocol/* tablestore/flatbuffer/dataprotocol/
rm -rf tablestore/flatbuffer/dataprotocol/tablestore

./bin/flatc-mac-22.9.24/flatc-mac  --python -o "tablestore/flatbuffer/timeseries"  tablestore/flatbuffer/timeseries.fbs
mv tablestore/flatbuffer/timeseries/tablestore/flatbuffer/timeseries/* tablestore/flatbuffer/timeseries/
rm -rf tablestore/flatbuffer/timeseries/tablestore
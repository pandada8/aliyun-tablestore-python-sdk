# -*- coding: utf8 -*-

MaxInstanceNameLength = 16           # The upper limit of Instance name length
MaxTableNameLength = 255             # The upper limit of the Table name length
MaxColumnNameLength = 255            # Maximum length of column names
MaxInstanceCountForUser = 5          # The upper limit of Instances contained in an account
MaxTableCountForInstance = 10        # The upper limit of the number of tables contained in an Instance
MaxPKColumnNum = 4                   # Upper limit of the number of columns included in the primary key
MaxPKStringValueLength = 1024        # Upper limit of String type column size (primary key column)
MaxNonPKStringValueLength = 64 * 1024# Upper limit of column size for String type (non-primary key column)
MaxBinaryValueLength = 64 * 1024     # Upper limit for the size of Binary type column values
MaxColumnCountForRow = 100           # Maximum number of columns in a row
MaxColumnDataSizeForRow = 1024 * 1024 # The upper limit of the total size of columns in a row
MaxReadWriteCapacityUnit = 5000      # Upper limit of Capacity Unit on the table
MinReadWriteCapacityUnit = 0         # Lower limit of Capacity Unit on the table
MaxRowCountForMultiGetRow = 100      # Upper limit on the number of rows for a single MultiGetRow operation
MaxRowCountForMultiWriteRow = 200    # The upper limit of the number of rows for a single MultiWriteRow operation
MaxRowCountForGetRange = 5000        # Maximum number of rows returned by a single Query
MaxDataSizeForGetRange = 1024 * 1024 # Upper limit of the data size returned by a single Query
MaxCUReduceTimeLimit = 4             # Maximum number of CU downscaling attempts

CUUpdateTimeLongest = 60             # Longest response time for UpdateTableCU, unit: s

CURestoreTimeInSec = 1               # This is not a restriction item, built-in variable, CU recovery time

# Spark.TableStatsExample
Simple Spark example of generating table stats for use of data quality checks

##SimpleDataGeneratorMain
This will generate small test data set

SimpleDataGeneratorMain {outputPath}

##ConfigurableDataGeneratorMain
This will generate large test data set

ConfigurableDataGeneratorMain {outputPath} {numberOfColumns} {numberOfRecords} {numberOfPartitions}

##TableStatsSinglePathMain
This will output the following information on a given column in the table

* null count
* empty count
* total count
* unique value count
* max 
* min
* sum
* top N values with there cardinality

TableStatsSinglePathMain {inputPath}
# H1 Assignment


Note: Have some issues in writing data to Local FileSystem in personnel PC, hence not tested data writing functionality.
Tested locally in PyCharm with show(), below commands can be used to run with spark-submit command locally or in cluster .

#Driver
Requires atleast one parameter(ingest/preprocess/process)

#Ingestion

tasks/ingest.py
```
spark-submit --py-files packages.zip --files config/paths.json h1_driver.py ingest
```

#Preprocess

tasks/preprocess.py
```
spark-submit --py-files packages.zip --files config/paths.json h1_driver.py preprocess
```

#Process

tasks/process.py
```
spark-submit --py-files packages.zip --files config/paths.json h1_driver.py process
```

#UnitTests
Implemented partially

h1_unit_tests.py

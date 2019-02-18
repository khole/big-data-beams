# big-data-beams

NOTES:
Used Apache Beams
  - to run application
    1. RUN: export MAVEN_OPTS="-Xmx12000m -XX:-UseGCOverheadLimit -XX:+UseStringDeduplication"
    2. RUN: mvn compile exec:java -Dexec.mainClass=khole.beams.TaxiTripPipeline
  - used Windowing to subdivide a PCollection into bounded sets of elements
  - Couldn't process big json file due to OutOfMemoryError when trying to read big file
    - subdivided it first into 4 files ~1M per file
    - RUN: split -l 1000000 raw-data/large_df.json chunk_
  - Used Avro coder to convert JSON to a Java Object

DataSet Corrections:
Filter out incomplete records
Filter out zero or negative total and fare amounts
Filter out zero coordinates

TODO:
Data cleanup to remove coordinates in the sea and in South Pole
Improve Visualization in Tableau to relate time -> location -> total fare
 - so taxi drivers can know the best time locations to pick up passengers
Do Garbage Collection tuning to fix OutOfMemoryError

VISUALIATIONS
PICKUP
![Pickup](https://raw.githubusercontent.com/khole/big-data-beams/master/viz-pickup.png)

DROPOFF
![Pickup](https://raw.githubusercontent.com/khole/big-data-beams/master/viz-dropoff.png)        

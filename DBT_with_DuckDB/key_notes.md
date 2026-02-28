+ Always deactivate the Database before running dbt
+ dbt debug
  + You changed profiles.yml
  + You changed connection path
  + You suspect connection issues
+ dbt compile
  + You want to see the compiled SQL
  + You’re debugging Jinja (ref(), macros, etc.)
  + You want to inspect /target/compiled
+ dbt run
  + Builds the models when:  
    + SQL Model is valid
    + ref() is correct
+ dbt docs generate
  + creates documentation
  + Parses your project
  + Reads all models
  + Reads all ref() dependencies
  + Reads tests
  + Reads descriptions from .yml files
  + Builds a metadata graph
  + Writes documentation files into:
    + target/
      + manifest.json
      + catalog.json
      + index.html
+ dbt docs serve
  + Starts a local web server
  + Serves the generated documentation
  + Opens browser (usually http://localhost:8080)
  + you see:
    + DAG (model lineage graph)
    + Model SQL
    + Columns
    + Data types
    + Tests
    + Descriptions
    + Dependencies

Issues
INTERNAL Error: Failed to deserialize: expected end of object, but found field id: 101
DROP TABLE nyc_parking_violations.bronze.parking_violations;
DROP TABLE nyc_parking_violations.bronze.violation_codes;
DROP SCHEMA nyc_parking_violations.bronze

Python - Scripts
Rebuild Schema
Reingest the Data

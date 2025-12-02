# ELT workflow
- extract the data with python script from a API 
- load that data into a bucket (s3/minIO)
- transform using *dataframes*
  - duckdb through sql code, for cleaning and adapting raw data to the star schema of the postgresql database 
  - here is where are applied: WITH CTE, window functions, etc 
  - star schema: OLAP queries 
- BI/visualization: apache superset for do BI queries of the already transformed data 
# legibility vs composition in software projects 
- conceptual: the standarization and agreement in the approach writing the code is a early step
- high-frequency changing files = more legibility and less composition (FP) to improve debugging velocity 
  - trade-off: legibility + speed
- business requisites changing at a month frequency 
- the code should be **easy to modify**
## linting & formatting 
- linting & formatting criteria are formalized in .yaml, .json, .cfg files 
- and are automate through *linters*
## CI 
- yaml. files of github actions/gitlab ci formalize the politic of the project 
- exigen testing and lintering before merge 
## markdown files 
- CONTRIBUTING.md | STYLE_GUIDE.md
## functional programming 
- 'one-line': encapsulate the 'error scope' in the functions present 
- it leverage the *referential transparency*
- debug: 
  - inspect stdin/stdout of the function  
- debug OOP: 
  - error scope: global/temporal 
  - red of interactions between objects a lo largo del time 
  - recreate the *sequence of events* that condujeron to the defectuose state 

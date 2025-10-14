# 9th october 
## change the scope of the project 
- starting the documentation of the project 
- added mkdocs for funcionality improves 
### deprecated  
- created 'deprecated' dir, rename & mv there extract.py of static dataset 
- created new 'extract-alpha-vantage.py'
### branches 
- working out in main, change branch for better git workflow and standard practices 
## API format 
- always choose **json** format for raw data if is available 
  - later would be transformed into csv or parquet 
## ? in HTTP connections 
- ? is the *syntax sugar* that points the init of *query parameters*  
## .env files 
- NEVER use "" or '' in environment variables on .venv config files to simple values (URLs, API keys, numbers)
- cause: error parsing 
- 

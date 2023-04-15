# Database tool readme
## database_tool
### Usage and installation
#### Notes:
This document captures my experience
- installing PostgreSQL, 
- restoring the ontology database locally, 
- cloning the database_tool and setting up environment
- early uses of the database_tool

Some of these steps should probably be moved to separate documents. I mostly wanted to capture the required steps while they are all fresh in my mind. These should be useful for new developersor users, in the future.


---
## Ontology

### Database backup
**note** In order to You will need a database backup file. 
- This file can be created from pg_dump. 
- It can also be created by right-clicking on the database in pgAdmin and selecting backup
- **Note:** backup in pgAdmin uses pg_dump under the hood, so these are equivalent
  - you can use the default values
- A file with a sql extension will be created.

### PostgreSQL Installation
1. Download [PostgreSQL](https://www.postgresql.org/download/)
2. install PostgresSQL
   - You will be asked for a password during installation
   - The PostgreSQL installation will automatically install a user account named postgres.  
   - This is the admin account with superuser privileges.
   - The password entered above is for the postgres user.
3. pgAdmin 4 should be in the PostgreSQL scripts directory
   - Default directory: C:\Program Files\PostgreSQL\15\scripts
4. Make certain that C:\Program Files\PostgreSQL\15\scripts is in PATH


### Ontology database creation and setup
1. Open pgAdmin and create ontology database
    - In the left pane under
      - Servers
      - PostgreSQL 15
    - Right click on **Databases** and select create database
    - Name database **ontology**
    - Keep other default values
    - This will create an empty database
2. Open shell and change to the directory with the backup.sql file
3. Run the following command:
   - pg_restore -v -U postgres -d ontology backup.sql
   - The -U option selects the user during installation
   - You will be prompted for a password. Enter the password for postgres created during installation
useful link to [pg_dump documentation](https://www.postgresql.org/docs/current/app-pgrestore.html)
---
## Setup the database_tool
### Clone database tool to computer
### Setup virtual environment
1. cd to database_tool
2. python -m venv venv
3. venv\scripts\activate
4. pip install -r requirements.txt
---
## Setup database with new schema

### These directions will change after tool is complete:
### ToDo: *update the documentation*
1. cd to database_tool/build_database
2. python build_database.py --create
   - This will create a new database called *test*
   - **note** I need to change the name of the new schema database
3. python build_database.py --table
   - This will create empty tables using the new schema
   - **note** You can run python build_database --create --table to skip a step
4. python build_database.py --reflect --enumerate --check
   - This set of commands will first reflect the ontology database into the code. 
   - Then it will iterate over the tables and store them in a datastructure
   - last it will perform a series of consistency checks on the tables
5. python build_database.py --help for help on other commands

#### **Note** this last part is being worked and will almost certainly be refactored into something more streamlined


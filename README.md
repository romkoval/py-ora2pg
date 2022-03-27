# Oracle to postgresql migration tool
![usage demo](https://raw.githubusercontent.com/romkoval/images/master/ora2pg.gif)
 * prepare environment (optional step)
    * `# pyvenv3 ve-ora2pg`
    * `# source ve-ora2pg/bin/activate`

 * install package
    * `# pip install ora2pg`

 * usage   
    - example:
    ```
    ora2pg --replace-query \
                     "foo[select * from foo where bar='bar']" \
                     "foo2[select * from foo2 where bar='bar']" \
                     --use-copy \
                     -l foo,bar,example \
                     --log-file=/tmp/ora2pg.log \
                     pq://postgresql-connect-string oracle-connect-string
     ```
#### Speedup copying process
   Use `--processes` and `--use-copy` parameters to speedup copying large amount of data. `Processes` means number of processes to decode data for PG, **not** number of parallel queries.

#### Ora2Pg copy tables - help output
```
ora2pg [-h] [--truncate-tables] [--disable-triggers]
                 [--batch-copy-rowcount BATCH_ROWCOUNT]
                 [--table-list TABLES_TO_COPY] [--use-copy]
                 [--log-file LOG_FILE] [--exclude-list EXCLUDE_LIST]
                 [--skip-count]
                 [--replace-query [REPLACE_QUERY [REPLACE_QUERY ...]]]
                 [--force] [--processes PROCESSES] [--fk-drop] [--cmp]
                 [--cmp-tab-list] [--seq-last-number-fix]
                 pg_uri ora_uri
positional arguments:
  pg_uri                PG connect string, pq://...
  ora_uri               ORA connect string

optional arguments:
  -h, --help            show this help message and exit
  --truncate-tables, -z
                        truncate tables before copy
  --disable-triggers, -t
                        disable triggers before copy
  --batch-copy-rowcount BATCH_ROWCOUNT, -b BATCH_ROWCOUNT
                        number of rows to copy at once, default=6000
  --table-list TABLES_TO_COPY, -l TABLES_TO_COPY
  --use-copy            use PG COPY command to copy data
  --log-file LOG_FILE   log file, default=ora2pg.log
  --exclude-list EXCLUDE_LIST, -x EXCLUDE_LIST
                        Exclude table list (comma separated). Copy all tables
                        in schema excluding this list
  --skip-count          Do not perform counting rows before copy. Disables
                        progress bar.
  --replace-query [REPLACE_QUERY [REPLACE_QUERY ...]]
                        replase query for table, format: table_name[select *
                        from table_name where cond=some_value]
  --force               Don't ack, just do
  --processes PROCESSES
                        Number of processes to decode data to COPY in PG,
                        default=1
  --fk-drop, -f         Drop foreign keys in PG and exit
  --cmp                 Count rows in PG & ORA DBs and exit
  --cmp-tab-list        Compare table list - user input and oracle user_tables
                        and exit
  --seq-last-number-fix
                        Update sequences last numbers and exit
```


#### Create Postgresql DB schema by Oracle schema
```
usage: gen_pg_tabs [-h] [-l OBJECT_LIST] [-v] [-p] [-f] [-t] [-i] [-s]
                      [-d DEST_DIR] [-m]
                      connect_string

positional arguments:
  connect_string        ORACLE connect string as for SQL Plus

optional arguments:
  -h, --help            show this help message and exit
  -l OBJECT_LIST, --object-list OBJECT_LIST
                        object list to export, comma separate names
  -v, --verbose
  -p, --primary-keys    add primary keys directives in to create table
                        definition
  -f, --foreign-keys    add foreign keys directives in to create table
                        definition
  -t, --export-tables   export tables
  -i, --export-indexes  export indexes
  -s, --export-sequences
                        export sequences
  -d DEST_DIR, --destination-dir DEST_DIR
                        save tables, indexes and etc in separate files under
                        DEST_DIR/1Tab, DEST_DIR/1Tind, ...
  -m, --sequence-strart-last-number

```

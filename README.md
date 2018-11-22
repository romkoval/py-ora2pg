# Oracle to postgresql migration tool

 * prepare environment
    * `# pyvenv3 ve-ora2pg`
    * `# pip install -r requirements.txt`
    * `# source ve-ora2pg/bin/activate`
 * usage
    - example:
    ```
    python ora2pg.py --replace-query "foo[select * from foo where bar='bar']" "foo2[select * from foo2 where bar='bar']" \
                     --log-file=/tmp/ora2pg.log -l foo,bar,example \
                     --use-copy pq://postgresql-connect-string oracle-connect-string
     ```

# Ora2Pg copy tables - help output
```
usage: ora2pg.py [-h] [--truncate-tables] [--disable-triggers]
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
                        coma separate list of tables to copy. Aliases: nsi,
                        sett, pnrs, arch, tlg, oths, all. Default=pnrs
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

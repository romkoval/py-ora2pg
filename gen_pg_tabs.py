#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""
    Exports Oracle schema to Postgres
    NOTE: No adoptation for packages & triggers
"""


import sys
import os
import os.path
import re
from argparse import ArgumentParser
import cx_Oracle


def ensure_directory(dname):
    """creates directory if it not exists"""
    if not os.path.exists(dname):
        os.makedirs(dname)


def normalize_fname(fname):
    """replaces to _ strange chars in filename te be created"""
    fname = fname.lower()
    fname = re.compile(r'[^a-z0-9\.\\/]').sub('_', fname)
    return fname


def init_db_conn(connect_string):
    """initializes database connection"""
    try:
        return cx_Oracle.connect(connect_string)
    except cx_Oracle.Error as ex:
        err_str = 'Connection error using %s: %s' % (connect_string, ex)
        print(err_str)
        return None


def select_qry(cur, querystr, parameters):
    """executes SQL SELECT query"""
    cur.execute(querystr, parameters)
    results = cur.fetchall()
    return results


def init_session(cur):
    """initialization of SQL session"""
    cur.execute("ALTER SESSION SET nls_numeric_characters = '.,'")


def get_indexes_dict(cur, table, get_pk):
    """ return indexes dict """
    indexes_qry = """SELECT uic.index_name, uic.column_name, ui.index_type,
uie.column_expression, ui.uniqueness, uic.column_position
FROM user_ind_columns uic
LEFT JOIN (user_indexes ui) ON uic.index_name = ui.index_name
LEFT JOIN (user_ind_expressions uie) ON uic.index_name = uie.index_name
WHERE uic.table_name=:tablename
AND (uic.index_name NOT IN (SELECT index_name FROM user_constraints
                            WHERE table_name=:tablename AND constraint_type='P') or :show_pk=1)
ORDER BY uic.index_name, uic.column_position
"""

    indexes = {}
    idx_uniques = {}
    for row in select_qry(cur, indexes_qry, {"tablename": table,
                                             "show_pk": (get_pk and 1 or 0)}):
        idx_name = row[0]
        if idx_name.startswith('SYS_'):
            continue
        idx_column = row[1]
        idx_type = row[2]
        idx_expression = row[3]
        idx_unique = row[4]
        if idx_unique == 'UNIQUE':
            idx_unique = ' UNIQUE'
        else:
            idx_unique = ''
        idx_uniques[idx_name] = idx_unique
        if idx_type == 'FUNCTION-BASED NORMAL':
            idx_column = idx_expression
        try:
            indexes[idx_name].append(idx_column)
        except KeyError:
            indexes[idx_name] = [idx_column, ]

    return indexes, idx_uniques


def dump_table_indexes(cur, opts, table):
    """returm table indices"""
    indexes_str = ''
    indexes, idx_uniques = get_indexes_dict(cur, table, not opts.pkeys_in_tab)
    if indexes:
        idxs = [idx for idx in indexes.keys()]
        idxs.sort()
        idx_lines = []
        for idx in idxs:
            idx_text = 'CREATE%s INDEX %s ON %s (%s);' % \
                        (idx_uniques[idx], idx, table, ', '.join(indexes[idx]))
            idx_lines.append(idx_text)
            dump_to_file(opts, '1Tind', table + '.' + idx, idx_text)
    return indexes_str


def get_primary_key_dict(cur, table):
    """information about primary key columns"""
    contr_qry = """SELECT uc.index_name, ucc.constraint_name, ucc.column_name
FROM user_constraints uc, user_cons_columns ucc
WHERE uc.constraint_name = ucc.constraint_name
AND uc.constraint_type = 'P'
AND uc.table_name=:table_name
order by POSITION
"""

    res = select_qry(cur, contr_qry, {"table_name": table})
    pk_columns = []
    constraint_name = None
    index_name = None
    for row in res:
        index_name = row[0].upper()
        constraint_name = row[1].upper()
        pk_columns.append(row[2].upper())

    if index_name:
        return {
            'index_name': index_name,
            'constraint_name': constraint_name,
            'pk_columns': pk_columns,
            'table': table
        }
    else:
        return None


def get_primary_key_ddl(cur, table):
    """ information about primary key columns """
    pk = get_primary_key_dict(cur, table)

    if pk:
        return 'CONSTRAINT %s PRIMARY KEY (%s)' % (pk['constraint_name'], ', '.join(pk['pk_columns']))
    else:
        return None


def get_foreign_keys_dict(cur, table):
    """returns dictionary with info about foreign keys"""

    foreign_keys_qry = """
SELECT uc.table_name, ucc.column_name, ucc.position
, fc.table_name, uic.column_position, uic.column_name
, uc.delete_rule, uc.constraint_name
FROM user_cons_columns ucc
,user_constraints fc
,user_constraints uc
,user_ind_columns uic
WHERE  uc.constraint_type = 'R'
AND    uc.constraint_name = ucc.constraint_name
AND    fc.constraint_name = uc.r_constraint_name
AND uic.index_name=fc.constraint_name
AND uc.table_name=:table_name
ORDER BY uc.constraint_name, ucc.position, uic.column_position
"""

    fk = {}
    res = select_qry(cur, foreign_keys_qry, {"table_name": table})
    for row in res:
        _, fk_col_name, fk_col_pos, tab_name, ind_col_pos, ind_col_name, delete_rule, fk_name = row
        try:
            if len(fk[fk_name][0]) == fk_col_pos - 1:
                fk[fk_name][0].append(fk_col_name)
            if len(fk[fk_name][2]) == ind_col_pos - 1:
                fk[fk_name][2].append(ind_col_name)
        except KeyError:
            fk[fk_name] = [[fk_col_name, ], [tab_name, ], [ind_col_name, ], [delete_rule, ]]
    return fk


def get_foreign_key_ddl(cur, table):
    """adds information about foreign keys"""
    fkd = get_foreign_keys_dict(cur, table)
    fkk = [k for k in fkd.keys()]
    fkk.sort()
    ret_cols = []
    for cname in fkk:
        fk_columns = fkd[cname][0]
        table2 = fkd[cname][1][0]
        ind_columns = fkd[cname][2]
        on_delete = fkd[cname][3][0]
        if on_delete == 'CASCADE':
            on_delete = 'ON DELETE CASCADE'
        else:
            on_delete = ''
        tmp_str = 'CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s' % \
                  (cname, ','.join(fk_columns), table2, ','.join(ind_columns), on_delete)
        ret_cols.append((cname, tmp_str))
    return ret_cols


def dump_foreign_keys(cur, opts, table):
    """ saves constraints """
    fk_columns = get_foreign_key_ddl(cur, table)
    for fkc in fk_columns:
        ddl = "ALTER TABLE %s ADD %s;" % (table, fkc[1])
        dump_to_file(opts, '2Constr', table + '.' + fkc[0], ddl)


def dump_primary_keys(cur, opts, table):
    """ saves pk constraints """
    pk = get_primary_key_dict(cur, table)
    if pk:
        ddl = 'ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY USING INDEX %s;' % (
            pk['table'], pk['constraint_name'], pk['index_name']
        )
        dump_to_file(opts, '2Constr', table + '.' + pk['constraint_name'], ddl)


def map_pg_number(length: str) -> str:
    """ maps PG number types """
    if length == ',' or length == ',0':
        return 'BIGINT'

    __p, __s = length.split(',')
    if __p and __s and __s != '0':
        return 'DECIMAL(%s,%s)' % (__p, __s)
    if not __s or __s == '0':
        pint = int(__p)
        if pint < 5:
            return 'SMALLINT'
        elif pint < 10:
            return 'INTEGER'
        elif pint < 19:
            return 'BIGINT'
        else:
            return "DECIMAL(%s)" % __p


def ora2pg_data_type(data_type: str, length: str, char_length: str) -> str:
    """ transforms oracle data_type to PG
        http://www.sqlines.com/oracle-to-postgresql
    """
    if data_type == 'NUMBER':
        return map_pg_number(length)
    elif data_type == 'VARCHAR2':
        return 'VARCHAR(%s)' % char_length
    elif data_type == 'DATE':
        return 'TIMESTAMP(0)'
    elif data_type.startswith('TIMESTAMP'):
        return data_type
    elif data_type == 'CLOB':
        return 'TEXT'
    elif data_type == 'FLOAT':
        return 'DOUBLE PRECISION'


def isKeyColname(colname):
    if colname.startswith('_') or colname.upper() in ['END', 'BEGIN']:
        return True
    else:
        return False


def table_info_row(row):
    """shows info about table column"""
    column_name = row[0]
    data_type = row[1]
    nullable = row[2]
    hasdef = row[3]
    data_length = row[4]
    data_default = row[5]
    char_length = row[6]
    default_str = nullable_str = ''
    pg_data_type = ora2pg_data_type(data_type, data_length, char_length)
    if int(hasdef) == 1:
        default_str = ' DEFAULT %s' % (data_default)
    if nullable == 'N':
        nullable_str = ' NOT NULL'
        if default_str.endswith(' '):
            nullable_str = 'NOT NULL'
    if isKeyColname(column_name):
        column_name = '"' + column_name + '"'
    else:
        column_name = column_name.upper()
    return '%(column_name)s %(data_type)s%(default)s%(nullable)s' % \
           {'column_name': column_name, 'data_type': pg_data_type,
            'nullable': nullable_str, 'default': default_str}


def create_create_table_ddl(cur, table, add_pk_cols, add_fk_cols):
    """creates DDL with CREATE TABLE for table"""
    table_cols_qry = """SELECT column_name, data_type, nullable,
decode(default_length, NULL, 0, 1) hasdef,
decode(data_type,
    'DATE', '11',
    'NUMBER', data_precision || ',' || data_scale,
    data_length) data_length,
    data_default,
    char_length
FROM user_tab_columns
WHERE table_name=:tablename
ORDER BY column_name
"""

    tab_cols = []
    for row in select_qry(cur, table_cols_qry, {"tablename": table}):
        tab_cols.append(table_info_row(row).strip())

    if add_pk_cols:
        pk_columns = get_primary_key_ddl(cur, table)
        if pk_columns:
            tab_cols.append(pk_columns)

    if add_fk_cols:
        fk_columns = get_foreign_key_ddl(cur, table)
        for fkc in fk_columns:
            tab_cols.append(fkc[1])

    # creates DDL CREATE TABLE instruction
    # \n, is required when column has comment
    create_tab_ddl = 'CREATE TABLE %s (\n    %s\n);' % (table.upper(), ',\n    '.join(tab_cols))
    return create_tab_ddl


def create_tab_col_comment_ddl(cur, table):
    """ tab column comments """
    col_comment_qry = """select COLUMN_NAME, COMMENTS
from USER_COL_COMMENTS
where TABLE_NAME = :tablename and comments is not null
order by column_name"""

    comments = []
    for row in select_qry(cur, col_comment_qry, {"tablename": table}):
        column, comment = row
        comments.append("COMMENT ON COLUMN %s.%s IS E'%s';" % (table, column, comment))

    return '\n'.join(comments)


def create_tab_comment_ddl(cur, table):
    """ table comment """
    tab_comment_qry = """select COMMENTS
from USER_TAB_COMMENTS
where TABLE_NAME = :tablename
and COMMENTS is not null"""
    comments = []
    for row in select_qry(cur, tab_comment_qry, {"tablename": table}):
        comment = row[0]
        comments.append("COMMENT ON TABLE %s IS E'%s';" % (table, comment))

    return '\n'.join(comments)


def dump_to_file(opts, obj_dir, obj_name, data):
    """saves object to file"""
    ensure_directory(os.path.join(opts.dest_dir, obj_dir))
    filename = os.path.join(opts.dest_dir, obj_dir, obj_name + '.sql')
    if opts.verbose:
        print("\n%s:\n%s" % (filename, data))
    with open(filename, 'w') as file:
        file.write(data)
        if data[-1] != '\n':
            file.write('\n')


def dump_sequences(cur, object_list, opts):
    """shows database sequences"""

    seq_qry = """SELECT sequence_name, min_value, max_value, increment_by,
                        last_number, cache_size, cycle_flag, order_flag
                 FROM user_sequences"""
    rows = select_qry(cur, seq_qry, {})

    for row in rows:
        sequence_name = row[0].upper()
        if object_list is None or sequence_name in object_list:
            if row[1] == 1:
                min_value = 'NO MINVALUE'
            else:
                min_value = 'MINVALUE %.0f' % row[1]

            if row[2] >= pow(2, 63)-1:
                max_value = 'NO MAXVALUE'
            else:
                max_value = 'MAXVALUE %.0f' % row[2]
            increment_by = '%.0f' % row[3]
            startswith_number = '%.0f' % (row[4] if opts.seq_start_with_lastnum else row[1])
            cache_size = '%.0f' % row[5]
            cycle_flag = row[6]
            # order_flag = row[7]

            if cache_size and cache_size != '0':
                cache_size = 'CACHE ' + cache_size
            else:
                cache_size = 'NOCACHE'

            if cycle_flag == 'Y':
                cycle_flag = 'CYCLE'
            else:
                cycle_flag = 'NO CYCLE'

            sequence_ddl = "CREATE SEQUENCE %s %s %s INCREMENT BY %s "\
                           "START WITH %s %s %s;\n" % \
                           (sequence_name, min_value, max_value, increment_by,
                            startswith_number, cache_size, cycle_flag)
            dump_to_file(opts, '3Seq', sequence_name, sequence_ddl)


def dump_tables_indexes(cur, object_list: list, opts: str):
    """ dumps table list """
    qry_str = """SELECT table_name
FROM user_tables
WHERE INSTR(table_name, 'X_') <> 1
AND INSTR(table_name, '$') = 0
AND NOT table_name IN (SELECT view_name FROM user_views)
AND NOT table_name IN (SELECT mview_name FROM user_mviews)
ORDER BY table_name
"""
    table_list = select_qry(cur, qry_str, {})
    for row in table_list:
        table_name = row[0].upper()
        if object_list is None or table_name in object_list:
            if opts.export_tabs:
                table_ddl = create_create_table_ddl(cur, table_name, opts.pkeys_in_tab, opts.fkeys_in_tab)
                table_comments_ddl = create_tab_comment_ddl(cur, table_name)
                table_column_comments_ddl = create_tab_col_comment_ddl(cur, table_name)

                dump_to_file(opts, '1Tab', table_name,
                             '\n'.join((table_ddl, table_comments_ddl, table_column_comments_ddl)))

            if opts.export_inds:
                dump_table_indexes(cur, opts, table_name)

            if not opts.fkeys_in_tab:
                dump_foreign_keys(cur, opts, table_name)
            if not opts.pkeys_in_tab:
                dump_primary_keys(cur, opts, table_name)


def dump_db_info(cur, stdout, object_list, opts):
    """ dump oracle schema to pg """
    if opts.export_tabs or opts.export_inds:
        dump_tables_indexes(cur, object_list, opts)

    if opts.export_seqs:
        dump_sequences(cur, object_list, opts)


def parse_prog_opts():
    """ parse input parameters """
    parser = ArgumentParser()
    parser.add_argument("-l", "--object-list", dest="object_list",
                        help="object list to export, comma separate names")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_argument("-p", "--primary-keys", action="store_true", dest="pkeys_in_tab",
                        help="add primary keys directives in to create table definition")
    parser.add_argument("-f", "--foreign-keys", action="store_true", dest="fkeys_in_tab",
                        help="add foreign keys directives in to create table definition")
    parser.add_argument("-t", "--export-tables", action="store_true", dest="export_tabs",
                        help="export tables")
    parser.add_argument("-i", "--export-indexes", action="store_true", dest="export_inds",
                        help="export indexes")
    parser.add_argument("-s", "--export-sequences", action="store_true", dest="export_seqs",
                        help="export sequences")
    parser.add_argument("-d", "--destination-dir", dest='dest_dir',
                        default='schema',
                        help="save tables, indexes and etc in separate files "
                             "under DEST_DIR/1Tab, DEST_DIR/1Tind, ... [default=%(default)s]")
    parser.add_argument("-m", "--sequence-strart-last-number", action="store_true",
                        dest="seq_start_with_lastnum",
                        help="Use sequence last_number for start value")
    parser.add_argument("connect_string", help="ORACLE connect string as for SQL Plus")

    opts = parser.parse_args()

    if opts.object_list is not None:
        opts.object_list = [obj.upper() for obj in opts.object_list.split(',')]

    export_all_objects = not (opts.export_inds or
                              opts.export_tabs or
                              opts.export_seqs)
    if export_all_objects:
        opts.export_inds = True
        opts.export_tabs = True
        opts.export_seqs = True

    return opts


def main():
    """main func"""
    opts = parse_prog_opts()
    stdout = sys.stdout

    ora_conn = init_db_conn(opts.connect_string)
    if not ora_conn:
        print('unable to connect to DB')
        return 1
    ora_cur = ora_conn.cursor()
    init_session(ora_cur)
    dump_db_info(ora_cur, stdout, opts.object_list, opts)
    ora_cur.close()


if __name__ == '__main__':
    sys.exit(main())

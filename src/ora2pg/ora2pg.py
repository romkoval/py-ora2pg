#!/usr/bin/env python3
# -*- coding: utf8 -*-import logging

import sys
import logging.handlers
from collections import namedtuple
from multiprocessing import Pool
import argparse
import postgresql # pip install py-postgresql
import cx_Oracle # pip install cx_Oracle
from tqdm import trange, tqdm

import datetime

########## https://github.com/python-postgres/fe/issues/106 ########
########## workaround ##############################################
import postgresql.versionstring as vs
def parse_debian_compat(vstr, _split=vs.split):
    return _split(vstr.split()[0])
vs.split = parse_debian_compat
########## workaround ##############################################

LOGGER = logging.getLogger(__name__)

def pg_table_fk_list(dbpg, tab):
    """ list of foreign keys on table """
    query = "select constraint_name from information_schema.table_constraints " \
            "where constraint_type='FOREIGN KEY' " \
            "and table_name = $1"
    LOGGER.debug('%s, tab=%s', query, tab.lower())
    dbq = dbpg.prepare(query)
    return [res['constraint_name'] for res in dbq(tab.lower())]

def pg_drop_fk_tab(dbpg, tab):
    """ drop fk on table """
    fk_list = pg_table_fk_list(dbpg, tab)
    for fk in fk_list:
        query = "alter table %s drop constraint %s" % (tab, fk)
        LOGGER.debug("%s, table=%s, fk=%s", query, tab, fk)
        dbpg.execute(query)


def pg_drop_fk(dbpg, table_list):
    """ drop PG FK on table list """
    for tab in table_list:
        pg_drop_fk_tab(dbpg, tab)

def pg_disable_triggers_tab(dbpg, tab):
    """ disable pg trigger """
    query = "ALTER TABLE " + tab + " DISABLE TRIGGER USER"
    LOGGER.debug(query)
    dbpg.execute(query)

def pg_truncate_tab(dbpg, tab):
    """ truncate dest table """
    query = "truncate table " + tab # + " cascade"
    LOGGER.debug(query)
    dbpg.execute(query)

def pg_enable_trigger_tab(dbpg, tab):
    """ enable trigger """
    query = "ALTER TABLE " + tab + " ENABLE TRIGGER USER"
    LOGGER.debug(query)
    dbpg.execute(query)

def get_count_rows_tab_cond(tab:str, args) -> str or None:
    if tab in args.replace_query:
        query = args.replace_query[tab].upper()
        return "select count(*) from " + query.split('FROM', 1)[1]
    return None

def ora_count_rows(curs, tab, args) -> int:
    """ source table rowcount """
    replaced_query = get_count_rows_tab_cond(tab, args)
    query = replaced_query if replaced_query else "select count(*) from " + tab
    LOGGER.debug("query=%s", query)
    curs.execute(query)
    return int(curs.fetchone()[0])

def pg_count_rows(dbpg, tab, args) -> int:
    """ pg table rowcount """
    query = "select count(*) count from " + tab
    LOGGER.debug("query=%s", query)
    qcount = dbpg.prepare(query)
    return qcount()[0]['count']

def get_ora_user_tabs(curs):
    """ returns all oracle user tables """
    query = "select table_name from user_tables"
    LOGGER.debug("query=%s", query)
    curs.execute(query)
    return [row[0] for row in curs.fetchall()]

def escape(data):
    """ pg escape data """
    if data is None:
        return '\\N'
    if not isinstance(data, str):
        data = str(data)
    return ''.join([ch if ch not in ['\b', '\f', '\n', '\r', '\t', '\v', '\\']
                    else '\\'+ch for ch in data])

def escape_row(ora_row):
    """ escape ora row for PG COPY """
    return ('\t'.join([escape(i) for i in ora_row]) + '\n').encode('utf-8')

def mask_col(col):
    """ mask PG kw columns """
    if col in ['END']:
        return '"%s"' % col
    return col

def ora_data2pg_copy(ora_rows, pool):
    """ oracle result rows to PG COPY string """
    if pool is None:
        return [escape_row(r) for r in ora_rows]
    else:
        return pool.map(escape_row, ora_rows)

def values_list(cols: [str], bin_cols: [str]) -> [str]:
    """
        >>> values_list(['col1', 'col2', 'col3'], ['col1', 'col3'])
        ['$1::bytea', '$2', '$3::bytea']
    """
    res = []
    for n, col in enumerate(cols):
        if col in bin_cols:
            res.append('$%d::bytea' % (n+1))
        else:
            res.append('$%d' % (n+1))
    return res

def encode_bin(data_rows, cols, bin_cols) -> list:
    """
        >>> encode_bin([('1', '2', '4', '\x01'), ('2','2','4','\x04')], ['col1', 'col2', 'col3', 'col4'], ['col4'])
        [('1', '2', '4', b'\\x01'), ('2', '2', '4', b'\\x04')]
    """
    res = []
    for row in data_rows:
        res_row = []
        for n, col in enumerate(cols):
            if col in bin_cols and row[n]:
                res_row.append(row[n].encode('cp866'))
            else:
                res_row.append(row[n])
        res.append(tuple(res_row))
    return res


def copy_table(curs, dbpg, tab, args):
    total_rows = 0 if args.skip_count else ora_count_rows(curs, tab, args)
    pbar = tqdm(desc=tab, total=total_rows)
    query = "select * from " + tab
    if tab in args.replace_query:
        query = args.replace_query[tab]

    LOGGER.debug(query)
    curs.execute(query)

    cols = [col[0] for col in curs.description]
    columns_masked = ','.join(['%s' % mask_col(col) for col in cols])

    if args.use_copy:
        pg_query = "copy " + tab + "(" + columns_masked + ") from STDIN"
    else:
        values = values_list(cols, args.bin_cols)
        pg_query = "insert into " + tab + "(" + columns_masked + ") " + \
                   "values (" + ','.join(values) + ")"

    LOGGER.debug(pg_query)
    ins = dbpg.prepare(pg_query)

    while True:
        uniq_error = False
        rows = curs.fetchmany(args.batch_rowcount)
        if not rows:
            break
        try:
            ins.load_rows(ora_data2pg_copy(rows, args.pool) \
                if args.use_copy else encode_bin(rows, cols, args.bin_cols))
        except postgresql.exceptions.UniqueError:
            LOGGER.error('UniqueError on batch insert.')
            uniq_error = True
        if uniq_error:
            for row in rows:
                try:
                    ins.load_rows(ora_data2pg_copy([row], args.pool) \
                        if args.use_copy else encode_bin([row], cols, args.bin_cols))
                    pbar.update(1)
                except postgresql.exceptions.UniqueError:
                    LOGGER.error('UniqueError on insert: %s', row)

        pbar.update(len(rows))
    pbar.close()


def copy_tables(curs, dbpg, args):
    """ copy tables """
    if args.processes > 1:
        args.pool = Pool(args.processes)
    else:
        args.pool = None

    for tab in args.tables_to_copy:
        copy_table(curs, dbpg, tab, args)

    if args.pool is not None:
        args.pool.close()
        args.pool.join()

def compare_tables(curs, dbpg, args):
    """ compare tables """
    counts = []
    for tab in args.tables_to_copy:
        counts += compare_table(curs, dbpg, tab, args)

    print('*'*40)
    for cnt in counts:
        if cnt.pg_count != cnt.ora_count:
            resstr = ("%s%s: ora: %d, pg: %d" % ("*" if cnt.usecond else " ", cnt.tablename,
                                                 cnt.ora_count, cnt.pg_count))
            LOGGER.debug(resstr)
            print(resstr)

    print("cmp done.")

def compare_table(curs, dbpg, tab, args) -> list:
    results = []
    RowCoundStruct = namedtuple('RowCoundStruct', 'tablename,usecond,pg_count,ora_count')
    has_cond = False
    sys.stdout.write(('*' if has_cond else ' ') + tab + '...')
    sys.stdout.flush()

    pg_count = pg_count_rows(dbpg, tab, args)
    ora_count = ora_count_rows(curs, tab, args)
    rcs = RowCoundStruct(tab, usecond=has_cond, pg_count=pg_count, ora_count=ora_count)
    results.append(rcs)
    if rcs.pg_count == rcs.ora_count:
        print("OK")
    else:
        print("ora: %d != pg: %d" % (rcs.ora_count, rcs.pg_count))

    return results


def pg_disable_triggers(dbpg, tables):
    """ disable triggers """
    for tab in reversed(tables):
        pg_disable_triggers_tab(dbpg, tab)

def pg_truncate_tabs(dbpg, tables):
    """ clear tables """
    for tab in reversed(tables):
        pg_truncate_tab(dbpg, tab)

def pg_enable_triggers(dbpg, tables):
    """ enable triggers """
    for tab in reversed(tables):
        pg_enable_trigger_tab(dbpg, tab)

def cmp_tab_list(curs, args):
    ora_tabs = get_ora_user_tabs(curs)
    for tab in args.tables_to_copy:
        if tab in ora_tabs:
            ora_tabs.remove(tab)
        else:
            print('extra: %s' % tab)
    print('not in list: %s' % (' '.join(ora_tabs)))

def reorder_tables(table_list):
    if 'HISTORY' in table_list:
        table_list.remove('HISTORY')
        table_list.insert(0, 'HISTORY')
    if 'TEXT_TLG_NEW' in table_list:
        table_list.remove('TEXT_TLG_NEW')
        table_list.insert(0, 'TEXT_TLG_NEW')
    if 'ARC_REF' in table_list:
        table_list.remove('ARC_REF')
        table_list.insert(0, 'ARC_REF')
    return table_list

def confirm_truncate_tabs():
    """ prompt user to truncate tabs """
    print("*"*80)
    answer = input('Truncate tables before copy? type "yes, please!" ... ')
    if answer == 'yes, please!':
        return True
    return False

def pg_get_seq_last_value(dbpg, seq_name) -> int:
    """ return last number pg seq """
    last_number = dbpg.prepare("select last_value from " + seq_name)
    return last_number()[0][0]


def pg_get_seqs(dbpg):
    seq_req = dbpg.prepare("""SELECT relname FROM pg_class WHERE relkind = 'S'""")
    return [seq[0].upper() for seq in seq_req()]

def pg_seq_last_number_fix(curs, dbpg):
    """ update sequences last numbers: ORA->PG """
    pg_seqs = pg_get_seqs(dbpg)

    seq_qry = """SELECT sequence_name, last_number FROM user_sequences"""
    curs.execute(seq_qry)
    for seq in curs.fetchall():
        seq_name, last_number = seq
        if seq_name not in pg_seqs:
            LOGGER.info('sequence %s not found in PG, ignore', seq_name)
            continue

        pg_last_number = pg_get_seq_last_value(dbpg, seq_name)
        if pg_last_number > last_number:
            LOGGER.info('sequence %s has value bigger then Ora [%d > %d], ignore', seq_name, pg_last_number, last_number)
            continue

        seq_alter = "alter sequence " + seq_name + " restart with " + str(last_number + 1)

        LOGGER.info(seq_alter)
        print(seq_alter)

        alter = dbpg.prepare(seq_alter)
        alter()

def main():
    """ main """
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    args = parse_arg()
    rotate_logfile(args.log_file)
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, filename=args.log_file)
    LOGGER.info(' '.join(sys.argv))

    LOGGER.debug('binary cols=%s', args.bin_cols)


    dbpg = postgresql.open(args.pg_uri)
    dbora = cx_Oracle.connect(args.ora_uri)

    curs = dbora.cursor()
    if args.exclude_list:
        args.tables_to_copy = get_ora_user_tabs(curs)
        for excl in args.exclude_list:
            if excl in args.tables_to_copy:
                args.tables_to_copy.remove(excl)

    if args.cmp_tab_list:
        cmp_tab_list(curs, args)
        return
    if args.seq_last_number_fix:
        pg_seq_last_number_fix(curs, dbpg)
        return
    if args.compare:
        compare_tables(curs, dbpg, args)
        return
    if args.drop_fk:
        pg_drop_fk(dbpg, args.tables_to_copy)
        return
    if args.disable_trigs:
        pg_disable_triggers(dbpg, args.tables_to_copy)

    if args.truncate_tabs:
        if args.force or confirm_truncate_tabs():
            pg_truncate_tabs(dbpg, args.tables_to_copy)
        else:
            print('Not confirmed, exiting...')
            return

    args.tables_to_copy = reorder_tables(args.tables_to_copy)

    copy_tables(curs, dbpg, args)

    if args.disable_trigs:
        pg_enable_triggers(dbpg, args.tables_to_copy)

    curs.close()

def tabs2list(tabs) -> list:
    "str list to list of str"
    if tabs:
        return tabs.replace(' ', '').replace('\n', '').upper().split(',')

def replace_query2dict(qlist: list('tab[query]')) -> dict:
    res_dict = dict()
    for q in qlist:
        q = q.strip()
        if not q.endswith(']') or '[' not in q:
            raise Exception('Format error, use: "table[query]", not: "%s"' % (q))
        tab, query = q.split('[', 1)
        res_dict[tab] = query[:-1]
    return res_dict

def parse_arg():
    """ parse program options """
    parser = argparse.ArgumentParser(description="Ora2Pg copy tables")
    parser.add_argument('--truncate-tables', '-z', dest='truncate_tabs', action='store_true',
                        help='truncate tables before copy')
    parser.add_argument('--disable-triggers', '-t', dest='disable_trigs', action='store_true',
                        help='disable triggers before copy')
    parser.add_argument('--batch-copy-rowcount', '-b', dest='batch_rowcount', type=int,
                        default=6000,
                        help='number of rows to copy at once, default=%(default)s')
    parser.add_argument('--table-list', '-l', dest='tables_to_copy', type=str,
                        help='coma separate list of tables to copy.')
    parser.add_argument('--binary-col', dest='bin_cols', action='append',
                        help='coma separate list of binary columns, use without --use-copy key')
    parser.add_argument('--use-copy', dest='use_copy', action='store_true',
                        help='use PG COPY command to copy data')
    parser.add_argument('--log-file', default='ora2pg.log', dest='log_file',
                        help='log file, default=%(default)s')
    parser.add_argument('--exclude-list', '-x', dest='exclude_list', type=str,
                        help='Exclude table list (comma separated). '
                             'Copy all tables in schema excluding this list')
    parser.add_argument('--skip-count', dest='skip_count', action='store_true',
                        help='Do not perform counting rows before copy. Disables progress bar.')
    parser.add_argument('--replace-query', nargs="*", dest='replace_query',
                        help='replase query for table, format: table_name[select * from table_name where cond=some_value]')
    parser.add_argument('--force', dest='force', action='store_true', help="Don't ack, just do")
    parser.add_argument('--processes', dest='processes', default='1', type=int,
                        help='Number of processes to decode data to COPY in PG, default=%(default)s')
    parser.add_argument('--fk-drop', '-f', dest='drop_fk', action='store_true',
                        help='Drop foreign keys in PG and exit')
    parser.add_argument('--cmp', dest='compare', action='store_true',
                        help='Count rows in PG & ORA DBs and exit')
    parser.add_argument('--cmp-tab-list', dest='cmp_tab_list', action='store_true',
                        help='Compare table list - user input and oracle user_tables and exit')
    parser.add_argument('--seq-last-number-fix', dest='seq_last_number_fix', action='store_true',
                        help='Update sequences last numbers and exit')
    parser.add_argument(dest='pg_uri', help='PG connect string, pq://...')
    parser.add_argument(dest='ora_uri', help='ORA connect string')

    args = parser.parse_args()
    args.tables_to_copy = tabs2list(args.tables_to_copy)
    if args.exclude_list is not None:
        args.exclude_list = tabs2list(args.exclude_list)

    if args.replace_query is not None:
        args.replace_query = replace_query2dict(args.replace_query)
    else:
        args.replace_query = {}

    if args.bin_cols is None:
        args.bin_cols = []
    return args

def backup_logfile_name(filename):
    filename_comp = filename.split('.')
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    if filename_comp[-1] == 'log':
        filename_comp = filename_comp[:-1] + [timestamp] + filename_comp[-1:]
    else:
        filename_comp.append(timestamp)
    return '.'.join(filename_comp)

def rotate_logfile(filename):
    """ move logfile """
    import os.path
    import os
    if os.path.isfile(filename):
        os.rename(filename, backup_logfile_name(filename))

if __name__ == '__main__':
    main()

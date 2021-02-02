import logging
import logging.handlers
import datetime
import sys
import argparse
from collections import namedtuple
import postgresql # pip install py-postgresql
import cx_Oracle # pip install cx_Oracle
from tqdm import trange, tqdm
from ora2pg import get_ora_user_tabs, backup_logfile_name, tabs2list, desc_table
from ora2pg import pg_count_rows,reorder_tables, get_update_timestamp_cond
from ora2pg import mask_col

LOGGER = logging.getLogger(__name__)

def clear_ora_data_by_cond(curs, tab, cond, timestamp):
    """ clear PG data by cond """
    query = 'delete from ' + tab + ' ' + cond

    LOGGER.debug(query)
    ora_args = {}
    if cond:
        ora_args = {":timestamp1": timestamp[0], ':timestamp2': timestamp[1]}
    curs.execute(query, ora_args)

def copy_table_cond(curs, dbpg, tab, cols, cond, args):
    cond_pg, cond_ora = cond
    LOGGER.debug("cond_ora: %s", cond_ora)

    columns = ','.join(cols)
    columns_masked = ','.join(['%s' % mask_col(col) for col in cols])

    if args.clear_tabs:
        clear_ora_data_by_cond(curs, tab, cond_ora, (args.timestamp_beg, args.timestamp_end))

    pbar = tqdm(desc=tab, total=pg_count_rows(dbpg, tab, cond_pg, \
                                           (args.timestamp_beg, args.timestamp_end)))
    query = "select " + columns_masked + " from " + tab + ' ' + cond_pg

    pg_args = ()
    if cond_pg:
        pg_args = (args.timestamp_beg, args.timestamp_end)
        LOGGER.debug("ora_args:%s", pg_args)

    LOGGER.debug(query)
    pgq = dbpg.prepare(query)

    ora_query = "insert into " + tab + "(" + columns + ") " + \
                "values (" + ','.join([":%d"%(i+1) for i in range(len(cols))]) + ")"

    LOGGER.debug(ora_query)

    for chunk in pgq.chunks(*pg_args):
        curs.executemany(ora_query, chunk, batcherrors=True)
        for errorObj in curs.getbatcherrors():
            print("Row", errorObj.offset, "has error", errorObj.message)
            LOGGER.error("Row %s has error %s", errorObj.offset, errorObj.message)
        pbar.update(len(chunk))
        curs.execute("commit")
    pbar.close()
    curs.execute("commit")


def copy_table(curs, dbpg, tab, cols, args):
    """copy table"""
    cond_ora = get_update_timestamp_cond(tab, (':timestamp1', ':timestamp2'), oracle=True)
    cond_pg = get_update_timestamp_cond(tab, ('$1', '$2'), oracle=False)

    for icond in range(len(cond_ora)):
        condition = (cond_pg[icond], cond_ora[icond])
        copy_table_cond(curs, dbpg, tab, cols, condition, args)

def copy_tables(curs, dbpg, args):
    """ copy tables """
    for tab in args.tables_to_copy:
        cols = desc_table(curs, tab)
        if not cols:
            LOGGER.error("Can't desc table %s", tab)
        else:
            copy_table(curs, dbpg, tab, cols, args)


def pg_get_seq_last_value(dbpg, seq_name) -> int:
    """ return last number pg seq """
    last_number = dbpg.prepare("select last_value from " + seq_name)
    return last_number()[0][0]

def ora_seq_last_number_fix(curs, dbpg):
    """ update sequences last number: PG->ORA """
    seq_qry = """SELECT relname FROM pg_class WHERE relkind = 'S'"""
    seq_list = dbpg.prepare(seq_qry)
    for seq in seq_list():
        seq_name = seq[0]
        last_value = pg_get_seq_last_value(dbpg, seq_name)
        ora_seq_qry = """SELECT last_number FROM user_sequences where sequence_name=:seq_name"""
        curs.execute(ora_seq_qry, {":seq_name": seq_name.upper()})
        ora_last_value_row = curs.fetchone()

        if ora_last_value_row is None:
            LOGGER.debug("no data in ORA for seq: %s", seq_name)

        ora_last_value = None if ora_last_value_row is None else ora_last_value_row[0]
        if ora_last_value and ora_last_value < last_value:
            curs.execute("alter sequence " + seq_name + " increment by " + str(last_value - ora_last_value + 1))
            curs.execute("select " + seq_name + ".nextval from dual")
            curs.fetchall()
            curs.execute("alter sequence " + seq_name + " increment by 1")
            LOGGER.debug("oracle seq %s.last_value incremented by %d", seq_name, last_value - ora_last_value + 1)
        else:
            LOGGER.debug("nothing to do with oracle seq %s", seq_name)

def ora_get_constraints(curs, table_name) -> list:
    """ list of foreign keys for the table_name """
    fk_query = """SELECT constraint_name
FROM ALL_CONSTRAINTS where
  table_name=:table_name
  and CONSTRAINT_TYPE = 'R'"""
    LOGGER.debug('execute: %s params=%s', fk_query, {":table_name": table_name})
    curs.execute(fk_query, {":table_name": table_name})
    return [row[0] for row in curs.fetchall()]

def ora_disable_fk(cur, tables_list):
    """ disable foreign key check on table list """
    for tab in tables_list:
        for constr in ora_get_constraints(cur, tab):
            cur.execute("ALTER TABLE " + tab + " DISABLE CONSTRAINT " + constr)

def ora_disable_triggers(cur, tables_list):
    """ disable all triggers on table list """
    for tab in tables_list:
        cur.execute("ALTER TABLE " + tab + " DISABLE ALL TRIGGERS")

def ora_enable_triggers(cur, tables_list):
    """ enable all triggers on table list """
    for tab in tables_list:
        cur.execute("ALTER TABLE " + tab + " ENABLE ALL TRIGGERS")

def main(args):
    """ main """
    dbpg = postgresql.open(args.pg_uri)
    dbora = cx_Oracle.connect(args.ora_uri)

    curs = dbora.cursor()
    if args.exclude_list:
        args.tables_to_copy = get_ora_user_tabs(curs)
        for excl in args.exclude_list:
            if excl in args.tables_to_copy:
                args.tables_to_copy.remove(excl)

    if args.seq_last_number_fix:
        ora_seq_last_number_fix(curs, dbpg)
        return

    if args.drop_fk:
        ora_disable_fk(curs, args.tables_to_copy)
        return
    if args.disable_trigs:
        ora_disable_triggers(curs, args.tables_to_copy)

    args.tables_to_copy = reorder_tables(args.tables_to_copy)

    copy_tables(curs, dbpg, args)

    if args.disable_trigs:
        ora_enable_triggers(curs, args.tables_to_copy)
    curs.close()

def parse_arg():
    """ parse program options """
    tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)) \
                .strftime('%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser(description="Ora2Pg copy tables")
    parser.add_argument('--pg-uri', '-p', dest='pg_uri',
                        default='pq://etick_test:etick@localhost/etick_test',
                        help='PG connect string, default=%(default)s')
    parser.add_argument('--ora-uri', '-o', dest='ora_uri',
                        default='etick_tst/etick_tst@10.1.5.221:25031/edu',
                        help='ORA connect string, default=%(default)s')
    parser.add_argument('--table-list', '-l', dest='tables_to_copy', type=str,
                        default='pnrs',
                        help='coma separate list of tables to copy. '
                             'Aliases: nsi, sett, pnrs, arch, tlg, oths, all. Default=%(default)s')
    parser.add_argument('--clear-tables', '-c', dest='clear_tabs', action='store_true',
                        help='clear dest tables before copy')
    parser.add_argument('--disable-triggers', '-t', dest='disable_trigs', action='store_true',
                        help='disable triggers before copy')
    parser.add_argument('--timestamp-beg', dest='timestamp_beg', type=str,
                        default='1981-06-17 00:00:00',
                        help='update (replace) data in tables after this timestamp, '
                             'default=%(default)s')
    parser.add_argument('--timestamp-end', dest='timestamp_end', type=str,
                        default='%s' % (tomorrow),
                        help='update (replace) data in tables before this timestamp, '
                             'default=%(default)s')
    parser.add_argument('--log-file', default='pg2ora.log', dest='log_file',
                        help='log file, default=%(default)s')
    parser.add_argument('--fk-drop', '-f', dest='drop_fk', action='store_true',
                        help='Drop foreign keys and exit')
    parser.add_argument('--exclude-list', '-x', dest='exclude_list', type=str,
                        help='Exclude table list (comma separated). '
                             'Copy all tables in schema excluding this list')
    parser.add_argument('--seq-last-number-fix', dest='seq_last_number_fix', action='store_true',
                        help='Update sequences last numbers and exit')

    args = parser.parse_args()
    args.tables_to_copy = tabs2list(args.tables_to_copy)
    if args.exclude_list is not None:
        args.exclude_list = tabs2list(args.exclude_list)

    try:
        args.timestamp_beg = datetime.datetime.strptime(args.timestamp_beg, '%Y-%m-%d %H:%M:%S')
        args.timestamp_end = datetime.datetime.strptime(args.timestamp_end, '%Y-%m-%d %H:%M:%S')

    except ValueError as exc:
        LOGGER.error(exc)
        exit(-1)
    return args


def rotate_logfile(filename):
    """ move logfile """
    import os.path
    import os
    if os.path.isfile(filename):
        os.rename(filename, backup_logfile_name(filename))

if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    args = parse_arg()
    rotate_logfile(args.log_file)
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, filename=args.log_file)
    LOGGER.info(' '.join(sys.argv))
    main(args)

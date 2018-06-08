import sqlite3

con = None
cur = None


def GetDBCur():
    global con, cur

    if cur:
        return cur

    con = sqlite3.connect("cfg.db", isolation_level=None)
    cur = con.cursor()
    sql = "create table if not exists logstat(filename text(256),offset integer,needread integer,date text(8))"
    cur.execute(sql)

    return cur

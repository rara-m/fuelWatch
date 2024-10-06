def raw_loader(df, conn, entity):
    cur = conn.cursor()
    a = [tuple(x) for x in df.collect()]
    b = ','.join(['%s'] * len(a))
    t = f'{entity}'
    q = "INSERT INTO {} VALUES {}".format(t, b)

    cur.execute(q, a)
    conn.commit()
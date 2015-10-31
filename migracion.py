import datetime
import time
import MySQLdb as mdb

con = mdb.connect('localhost', 'root', 'password', 'zenergy')
out = mdb.connect('localhost', 'root', 'password', 'sustentabilidad')

insert_query = "INSERT INTO dispositivos_datos(id, timestamp, valor, dispositivo_id, indicador_id) VALUES (DEFAULT, %s, %s, %s, %s)"

cur = con.cursor()
cur.execute("SELECT * from datos where id_dispositivo=7000 and instante is not NULL and valor is not NULL ")

cout = out.cursor()

rows = cur.fetchall()

for row in rows:
    timestamp = row[1]
    valor = row[4]
    if timestamp is None or valor is None:
        continue
    cout.execute(insert_query, (timestamp, valor, 2, 2))

out.commit()

if con:
    con.close()

if out:
    out.close()

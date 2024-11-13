import pymysql # type: ignore
import csv

db = pymysql.connect(host="localhost",    
                     user="root",         
                     passwd="root",  
                     db="ailos")


cur = db.cursor()
sql = "INSERT INTO CREDITOGESTAO_LIMITES_CHESPECIAL (IDRISCO_CARGA_CENTRAL, COOPERATIVA, NRCONTA, NRCONTRATO, TAXAJUROSMES, DATAINICIOVIGENCIA, DATAFINALVIGENCIA, VALORCONTRATO, VALORUTILIZADO, VALORNAOUTILIZADO, DATAREFERENCIA, SALDOS_JTS_OID, DHREGISTROTD) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
with open('dados-entrada.csv', newline='') as csvfile:
    next(csvfile)
    spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
    for row in spamreader:
        val = (row[0], row[1], row[2], row[3], row[4].replace(",", "."), row[5], row[6], row[7].replace(",", "."), row[8].replace(",", "."), row[9].replace(",", "."), row[10], row[11], row[12])
        cur.execute(sql, val)
db.commit() 
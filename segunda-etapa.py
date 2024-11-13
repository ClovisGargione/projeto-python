import pymysql # type: ignore

db = pymysql.connect(host="localhost",    
                     user="root",         
                     passwd="root",  
                     db="ailos")

mycursor = db.cursor()

mycursor.execute("SELECT distinct IDRISCO_CARGA_CENTRAL FROM CREDITOGESTAO_LIMITES_CHESPECIAL")

myresult = mycursor.fetchall()

sqlConsultaLimitesCEspecial = ("SELECT ID, case when COOPERATIVA = 1 then '89010971' " +
                              "when COOPERATIVA = 2 then '89201260' " +
                              "when COOPERATIVA = 3 then '89041110' " +
                              "when COOPERATIVA = 5 then '88811700' " +
                              "when COOPERATIVA = 6 then '88034050' " +
                              "when COOPERATIVA = 7 then '88020020' " +
                              "when COOPERATIVA = 8 then '88020000' " +
                              "when COOPERATIVA = 9 then '88075301' " +
                              "when COOPERATIVA = 10 then '88508190' " +
                              "when COOPERATIVA = 11 then '88307326' " +
                              "when COOPERATIVA = 12 then '89270000' " +
                              "when COOPERATIVA = 13 then '89287440' " +
                              "when COOPERATIVA = 14 then '85601630' " +
                              "when COOPERATIVA = 16 then '89140000' " +
                              "else '89041110' " +
                              "end as COOPERATIVA, " +
                              "IDRISCO_CARGA_CENTRAL, " +
                              "COOPERATIVA as IDCOOPERATIVA, " +
                              "NRCONTA, " +
                              "NRCONTRATO, " +
                              "ROUND((POWER(1 + (TAXAJUROSMES / 100),12) - 1) * 100,2) as TAXA, " +
                              "TAXAJUROSMES, " +
                              "DATAINICIOVIGENCIA, " +
                              "DATAFINALVIGENCIA, " +
                              "VALORCONTRATO, " +
                              "VALORUTILIZADO, " +
                              "VALORNAOUTILIZADO, " +
                              "DATAREFERENCIA, " +
                              "SALDOS_JTS_OID, " +
                              "case when VALORUTILIZADO > 0 then 104 else 103 end as cdproduto_contabil, " +
                              "case when VALORUTILIZADO > 0 then '0213' else '1902' end as cdmodalidade, " +
                              "case when VALORUTILIZADO > 0 then VALORUTILIZADO else VALORNAOUTILIZADO end as vlsaldo_limite, " +
                              "case when VALORUTILIZADO > 0 then VALORUTILIZADO else VALORNAOUTILIZADO end as vlcontabil_bruto, " +
                              "DHREGISTROTD FROM CREDITOGESTAO_LIMITES_CHESPECIAL WHERE IDRISCO_CARGA_CENTRAL = %s")

sqlInserirRiscoCargaOperacao = "INSERT INTO CREDITOGESTAO_RISCO_CARGA_OPERACAO (idrisco_carga_central, idCooperativa, nrdconta, nrcontrato, nrcepcon, nrtaxeft, dtinictr, vlcontrato, nrcontrato_principal, vljuros_suspenso, cdproduto_contabil, dtvencimento, cdmodalidade, vlsaldo_limite, cdCif, vlcontabil_bruto, flativo_problematico, id_creditogestao_limites_chespecial) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
for x in myresult:
  adr = (x)
  mycursor.execute(sqlConsultaLimitesCEspecial, adr)
  result = mycursor.fetchall()
  for row in result:
    #Limite Utilizado
    if row[11] > 0:
        val = (row[2], row[3], row[4], row[5], row[1], row[6], row[8], row[10], row[5], row[14], row[15], row[9], row[16], row[17], 1, row[18], 0, row[0])
        mycursor.execute(sqlInserirRiscoCargaOperacao, val)
    #Limite não Utilizado
    if row[12] > 0:    
        val = (row[2], row[3], row[4], row[5], row[1], row[6], row[8], row[10], row[5], row[14],  row[15], row[9], row[16], row[17], 1, row[18], 0, row[0])
        mycursor.execute(sqlInserirRiscoCargaOperacao, val)
db.commit()         


sqlConsultaOperacaoCEspecial = ("select *, case when CREDITOGESTAO_LIMITES_CHESPECIAL.VALORNAOUTILIZADO > 0 then " +
					            "case when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAINICIOVIGENCIA) <= 360 then 20 else 40 end " +
			                    "when CREDITOGESTAO_LIMITES_CHESPECIAL.VALORUTILIZADO > 0 then " +	
					            "case when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 30 then 110 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 30 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 60 then 120 " +
						        "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 60 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 90 then 130 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 90 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 180 then 140 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 180 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 360 then 150 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 360 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 720 then 160 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 720 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 1080 then 165 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 1080 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 1440 then 170 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 1440 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 1800 then 175 "  +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 1800 and DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) <= 5400 then 180 " +
                                "when DATEDIFF(CREDITOGESTAO_LIMITES_CHESPECIAL.DATAFINALVIGENCIA, CREDITOGESTAO_LIMITES_CHESPECIAL.DATAREFERENCIA) > 5400 then 190 " +
						        " end " +
                                " end as CDVENCIMENTO, " +
                                " case when CREDITOGESTAO_LIMITES_CHESPECIAL.VALORUTILIZADO > 0 then CREDITOGESTAO_LIMITES_CHESPECIAL.VALORUTILIZADO " +
						        " when CREDITOGESTAO_LIMITES_CHESPECIAL.VALORNAOUTILIZADO > 0 then CREDITOGESTAO_LIMITES_CHESPECIAL.VALORNAOUTILIZADO " +
                                " end as vlvencimento " +
                                " from  CREDITOGESTAO_RISCO_CARGA_OPERACAO inner join CREDITOGESTAO_LIMITES_CHESPECIAL on CREDITOGESTAO_RISCO_CARGA_OPERACAO.id_creditogestao_limites_chespecial = CREDITOGESTAO_LIMITES_CHESPECIAL.ID")

mycursor.execute(sqlConsultaOperacaoCEspecial)

resultConsultaOperacaoCEspecial = mycursor.fetchall()

sqlInserirRiscoCargaOperacao = "INSERT INTO CREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO (idrisco_carga_central, cdcarga_operacao, cdvencimento, vlvencimento) VALUES (%s, %s, %s, %s)"
for z in resultConsultaOperacaoCEspecial:
   val = (z[1], z[0], z[56], z[57])
   mycursor.execute(sqlInserirRiscoCargaOperacao, val)
db.commit()   
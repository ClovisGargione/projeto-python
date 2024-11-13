import pymysql # type: ignore
import csv
import locale
import datetime

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

db = pymysql.connect(host="localhost",    
                     user="root",         
                     passwd="root",  
                     db="ailos")

mycursor = db.cursor()

sqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO = "select * from CREDITOGESTAO_RISCO_CARGA_OPERACAO"

mycursor.execute(sqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO)

resultConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO = mycursor.fetchall()

with open('silver-operacao.csv', 'w') as operacaofile:
    wr = csv.writer(operacaofile, delimiter=';', quoting=csv.QUOTE_NONE)
    wr.writerow(["cdcarga_operacao","idrisco_carga_central","idCooperativa", "nrdconta", "nrcontrato", "dsctacos", "dsorgrec", "nrtaxidx", "nrperidx", "nrvarcam", "nrcepcon", "nrtaxeft", "dtinictr", "cdnatope", "dscaresp", "vlcontrato", "flprejuz", "qtdiaatr", "tpcartao", "qtparcela", "nrcontrato_principal", "dtsaida", "tpcontrato", "vljuros_suspenso", "cdproduto_contabil", "dtvencimento", "dtproxima_parcela", "vlproxima_parcela", "cdmodalidade", "cdrisco_refinanciamento", "vlsaldo_limite", "idsistema_origem", "cdCif", "vlcontabil_bruto", "qtparcela_paga", "vlparcela_paga", "nrversao_contrato", "tprenegociacao", "pedesconto_renegociacao", "flativo_problematico", "vlperda_acumulada", "dhregistro"])
    for r in resultConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO:
        values = (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], locale.format_string('%.2f', r[11]), r[12], r[13], r[14], locale.format_string('%.2f', r[15]), r[16], r[17], r[18], r[19], r[20], r[21], r[22], locale.format_string('%.2f', r[23]), r[24], r[25], r[26], r[27], r[28], r[29], locale.format_string('%.2f', r[30]), r[31], r[32], locale.format_string('%.2f', r[33]), r[34], r[35], r[36], r[37], r[38], r[39], r[40], datetime.datetime.now())
        wr.writerow(values)

sqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO = "select idrisco_carga_central, cdcarga_operacao, cdvencimento, vlvencimento from CREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO"

mycursor.execute(sqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO)

resultsqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO = mycursor.fetchall()

with open('silver-vencimentos.csv', 'w') as vencimentofile:
    wr = csv.writer(vencimentofile, delimiter=';', quoting=csv.QUOTE_NONE)
    wr.writerow(["idrisco_carga_central","cdcarga_operacao","cdvencimento", "vlvencimento", "dhregistro"])
    for r in resultsqlConsultarCREDITOGESTAO_RISCO_CARGA_OPERACAO_VENCIMENTO:
        values = (r[0], r[1], r[2], locale.format_string('%.2f', r[3]), datetime.datetime.now())
        wr.writerow(values)
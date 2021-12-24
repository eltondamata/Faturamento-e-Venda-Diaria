#elton.mata@martins.com.br

#Importa as bibliotecas e conecta no Oracle dwh01
import pandas as pd
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn
from dateutil.relativedelta import relativedelta
pd.options.display.float_format = '{:,.2f}'.format

#Parametros
ANOMES = int((pd.to_datetime("today") + relativedelta(months=-1)).strftime("%Y%m")) #Mes Anterior
print('ANOMES:', ANOMES)
#ANOMES = 202111

#Consulta
faturamento=(f"""     
SELECT DIMPOD.NUMANOMESDIA,
       DIMCNLVND.CODCNLVND,
       SUM(FTOFAT.VLRFATLIQ) AS VLRFATLIQ,
       SUM(FTOFAT.VLRRCTLIQAPU) AS VLRRCTLIQAPU,
       SUM(FTOFAT.VLRMRGBRT) AS VLRMRGBRT
   FROM DWH.FTOFAT FTOFAT
      , DWH.DIMPRD DIMPRD
      , DWH.DIMCLIEND DIMCLI
      , DWH.DIMFIL DIMFILLGT
      , DWH.DIMPOD DIMPOD
      , DWH.DIMTIP DIMTIP
      , DWH.DIMCNLVND DIMCNLVND
      , DWH.DIMFIL DIMFILEPD
      , DWH.DIMTIP TIPFRT
      , DWH.DIMFIL FILFAT
      , DWH.DIMTETVND TETVND
   WHERE FTOFAT.SRKPRD = DIMPRD.SRKPRD 
     AND FTOFAT.SRKCLIENDVND = DIMCLI.SRKCLIEND 
     AND FTOFAT.SRKFILLGT = DIMFILLGT.SRKFIL 
     AND FTOFAT.SRKDATFAT = DIMPOD.SRKPOD 
     AND FTOFAT.SRKTIPFAT = DIMTIP.SRKTIP 
     AND FTOFAT.SRKCNLVNDINI = DIMCNLVND.SRKCNLVND 
     AND FTOFAT.SRKFILEPD = DIMFILEPD.SRKFIL 
     AND FTOFAT.SRKTIPFRT = TIPFRT.SRKTIP 
     AND FTOFAT.SRKFILFAT = FILFAT.SRKFIL
     AND FTOFAT.SRKTETVND = TETVND.SRKTETVND     
     AND DIMTIP.CODTIP IN ('DVL','VNDMER') 
     AND DIMPOD.NUMANOMES = {ANOMES}
 GROUP BY DIMPOD.NUMANOMESDIA,
          DIMCNLVND.CODCNLVND
 """)
 
venda = (f"""     
SELECT DISTINCT t2.NUMANOMESDIA,
       t8.CODCNLVND,
       SUM(t1.VLRVNDEFTFAT) AS VENDA,
       SUM(t1.VLRRCTLIQ) AS VLRRCTLIQ,
       SUM(t1.VLRMRGBRT) AS VLRMRGBRT
      FROM DWH.FTOPED t1
      , DWH.DIMPOD t2
      , DWH.DIMCLIEND t3
      , DWH.DIMSTA t4
      , DWH.DIMSTA STAPRD
      , DWH.DIMTIP OPEFSC
      , DWH.DIMPRD t5
      , DWH.DIMFIL t6
      , DWH.DIMTETVND t7
      , DWH.DIMCNLVND t8
      WHERE (t1.SRKDATPED = t2.SRKPOD AND t1.SRKCLIENDVND = t3.SRKCLIEND AND t1.SRKSTAPED = t4.SRKSTA AND t1.SRKSTAPRD = 
           STAPRD.SRKSTA AND t1.SRKTIPOPEFSC = OPEFSC.SRKTIP AND t1.SRKPRD = t5.SRKPRD AND t1.SRKFILEPD = t6.SRKFIL 
		AND t1.SRKTETVND = t7.SRKTETVND 
		AND t8.SRKCNLVND = t1.SRKCNLVNDINI) 
		AND t4.CODSTA NOT IN ('4','5') 
		AND OPEFSC.CODTIP NOT IN ('8', '16') 
		AND t2.NUMANOMES = {ANOMES}
      GROUP BY  t2.NUMANOMESDIA,
          t8.CODCNLVND
  """)
dffat = pd.read_sql(faturamento, con=conn)
dfvnd = pd.read_sql(venda, con=conn)
conn.close()

#DEPARA = {8: 'EFACIL',9: 'EFACIL',5: 'VENDAS DIGITAIS',7: 'VENDAS DIGITAIS'}
DEPARA = {8: 'EFACIL',9: 'EFACIL'}
dffat['TIPO_CANAL'] = dffat['CODCNLVND'].map(DEPARA).fillna('ATACADO')
dfvnd['TIPO_CANAL'] = dfvnd['CODCNLVND'].map(DEPARA).fillna('ATACADO')

print('==> FATURAMENTO')
dffatdia = dffat.groupby(['NUMANOMESDIA', 'TIPO_CANAL'])['VLRFATLIQ'].sum().unstack()
dffatdia.loc[:,'TOTAL'] = dffatdia.iloc[:,-2:].sum(axis=1)
dffatdia.columns.name = None
dffatrlmb = dffat.groupby(['NUMANOMESDIA'])[['VLRRCTLIQAPU', 'VLRMRGBRT']].sum()
dffatdia = pd.merge(dffatdia.reset_index(), dffatrlmb.reset_index())
dffatdia = dffatdia.set_index('NUMANOMESDIA')
dffatdia.loc['TOTAL'] = dffatdia.sum(axis=0)
dffatdia.eval('MB=VLRMRGBRT/VLRRCTLIQAPU*100', inplace=True)
dffatdia.drop(columns=['VLRMRGBRT', 'VLRRCTLIQAPU'], inplace=True)
print(dffatdia.to_markdown(tablefmt='github', floatfmt=',.2f'), '\n')

print('==> VENDA')
dfvnddia = dfvnd.groupby(['NUMANOMESDIA', 'TIPO_CANAL'])['VENDA'].sum().unstack()
dfvnddia.loc[:,'TOTAL'] = dfvnddia.iloc[:,-2:].sum(axis=1)
dfvnddia.columns.name = None
dfvndrlmb = dfvnd.groupby(['NUMANOMESDIA'])[['VLRRCTLIQ', 'VLRMRGBRT']].sum()
dfvnddia = pd.merge(dfvnddia.reset_index(), dfvndrlmb.reset_index())
dfvnddia = dfvnddia.set_index('NUMANOMESDIA')
dfvnddia.loc['TOTAL'] = dfvnddia.sum(axis=0)
dfvnddia.eval('MB=VLRMRGBRT/VLRRCTLIQ*100', inplace=True)
dfvnddia.drop(columns=['VLRMRGBRT', 'VLRRCTLIQ'], inplace=True)
print(dfvnddia.to_markdown(tablefmt='github', floatfmt=',.2f'), '\n')

dffatdia = dffatdia.reset_index()
dfvnddia = dfvnddia.reset_index()

#Exportar dados para arquivo.csv
#df.to_csv(r'd:\a\Export.csv', sep=";", encoding="iso-8859-1", decimal=",", float_format='%.2f', date_format='%d/%m/%Y', index = False)

#envia email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from pretty_html_table import build_table
from envia_mail import server

address_book = ['elton.mata@martins.com.br']
sender = 'elton.mata@martins.com.br'
subject = f"Faturamento e Venda diaria {ANOMES}"
tabfat = build_table(dffatdia, 'blue_light', text_align='right')
tabvnd = build_table(dfvnddia, 'blue_light', text_align='right')

body = f"""<html><body>
<p>Faturamento...</p>
{tabfat}
<p>Venda...</p>
{tabvnd}
<p><i>(email automático)</i></p>
</body></html>
"""

msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = ','.join(address_book)
msg['Subject'] = subject
msg.attach(MIMEText(body, 'html'))
text=msg.as_string()
server.sendmail(sender,address_book, text)
server.quit()
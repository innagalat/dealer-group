import pandas as pd
import os
import pypyodbc


PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data')
RESULT = os.path.join(PROJ, 'output-files')



pypyodbc.lowercase = False

conn = pypyodbc.connect('DRIVER={SQL Server Native Client 10.0};'+
            'Server=WP200A025V; DATABASE =dbo.stg_ftx_risk;Trusted_Connection=Yes')


conn2 = pypyodbc.connect('DRIVER={SQL Server Native Client 10.0};'+
            'Server=WP200A025V; DATABASE =dbo.stg_ftx_pepl;Trusted_Connection=Yes', ansi=True)


qry = 'SELECT policy_id, dres, endd, stat, statd,'+\
                'CAST( (CAST(policy_id AS VARCHAR) + CAST(RTRIM(life_no) AS VARCHAR) + CAST(RTRIM(bene_no) AS VARCHAR)) AS bigint) AS POL_NO '+\
                'FROM dbo.stg_ftx_risk' #life_no, bene_no, dres
# CAST modifies the type of the value, RTRIM- trims value from the right by removing empty spaces

qry2 = "SELECT id, CAST(name1_mixed_case AS VARCHAR) AS 'First Name', "+\
				"CAST(name2_mixed_case AS VARCHAR) AS 'Last Name' FROM dbo.stg_ftx_pepl"



#________________________ print 10 rows from the query
cur = conn.cursor() 
cur.execute(qry);
count = 0
# while True:
while count < 10:
    count = count + 1
    row = cur.fetchone()
    if row is None:
        break
    print(u"policy_id: {0}, life_no: {1}, bene_no:{2}, POL_NO: {0}{1}{2} and dres: {3} and POL_NO: {4}".format(
            row.get("policy_id"), row.get("life_no"), row.get("bene_no"),row.get("dres"), row.get('POL_NO') ))
cur.close()



#________________________ print 10 rows from the query
cur2 = conn2.cursor() 
cur2.execute(qry2);
count = 0
# while True:
while  count < 1120:
    count = count + 1
    row = cur2.fetchone()
    if row is None:
        break
    print(count, u"id: {0}, Name: {1}, Surname:{2}".format(
            row.get("id"), row.get("First Name"), row.get("Last Name") ))
cur2.close()




# #________________________ save qry to df
# df = pd.read_sql(qry, conn) 
# print df.head(5)
# df.to_csv(RESULT +'\\FelixRisk(table)frompyodbc.csv', index = False)

# #________________________ close connection
# conn.close()


# # ________________________ save qry to df
# df = pd.read_sql(qry2, conn2) 
# print df.head(5)
# df.to_csv(RESULT +'\\FelixRiskPepl(table)frompyodbc.csv', index = False)


#________________________ close connection
# conn2.close()


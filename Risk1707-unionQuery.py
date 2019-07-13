# to paste this code to the Risk17AllAgentsBeneffDate..(WOP)_Query, so this one no need to be used


# DII1707short and Term1707short
#RISK1707short is below (union)
#RISK1707Query is below (make-table)

# creating a FELIX AGENT TABLEa
# checked, except of "removed spaces" all good, only one case: PREM_IND, which looks like 00LRCNIF0053F. It might be irrelevant


import pandas as pd
import numpy as np
from datetime import datetime
import os


# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'


print DCS_VAL_DATE[2:6]



PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')

parser = lambda x: pd.to_datetime(x, format='%d/%m/%Y', errors = 'coerce')
parser2 = lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors = 'ignore')


# Creating Policy number by removing benefits and checking that policies are WOP (11 digits)

def dii_term_change_struc_toSHORT (df):
	for i in range(0, df.shape[0]):
		if len(str(df.ix[i,'POL_NO'])) != 11:
			print ("ERROR: Attention! Pol_NO is longer than 11 digits")
			exit('1')
	df['PolicyNo'] = df['POL_NO'].astype(str).str[:7]	

	#rename column and drop columns that are not being used
	df.rename(columns = {'GROUPX':'GROUP', 'START_MONTH':'START_MNTH'}, inplace = True)
	df.drop(['ACC_BAL', 'BONUS', 'COST_BONUS', 'EMP_PERC', 'GRID_SCORE', 'IS', 
					'LIAB_GROSS', 'LIAB_REIN', 'SV', 'TAX_ID'],axis=1, inplace=True)

	return df


 # ______________________________________________
# may need to specify types of columns
df_Term = pd.read_table(DATA + '\\Term' + DCS_VAL_DATE[2:6] +'.DEL', sep =',')

# ______________________________________________
# may need to specify types of columns
df_Dii = pd.read_table(DATA +'\\Dii' + DCS_VAL_DATE[2:6] +'.DEL', sep =',')

print df_Term.shape[0]+df_Dii.shape[0]



# removes spaces between words
for i in df_Term.columns.values:
	if type(df_Term.ix[0,i]) == str:
		df_Term.ix[:,i] = [x.replace(' ', '') for x in df_Term.ix[:,i]]
for i in df_Dii.columns.values:
	if type(df_Dii.ix[0,i]) == str:
		df_Dii.ix[:,i] = [x.replace(' ', '') for x in df_Dii.ix[:,i]]



df_Term = dii_term_change_struc_toSHORT(df_Term)
df_Dii = dii_term_change_struc_toSHORT(df_Dii)
# ______________________________________________


#RISK1707short is below

if (set(df_Term.columns) != set(df_Dii.columns)):
	print("Attention! Merging may be incorrect. Check that columns of both files are the same")
	exit('1')

df = pd.concat([df_Term, df_Dii])

#RISK1707Query is below

df['PolLifeNo'] = df['POL_NO'].astype(str).str[:9]
print df.head(6)


df.to_csv(RESULT + '\\RiskYYMMQuery(table).csv', index = False)

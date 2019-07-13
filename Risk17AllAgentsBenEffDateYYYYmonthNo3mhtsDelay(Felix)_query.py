#Risk17AllAgentsBenEffDate2017JunNo3mhtsDelay(Felix) query
	#reads FelixRiskwithLifeNoBenNo straight from db
	# to FelixAgent_query adds to left columns from FelixRiskwithLifeNoBenNo@YYYYMMDD(table)


import pandas as pd

# for df_FelixRiskwithLife I am using table done by access named FelixRiskwithLifeNoBenno@20170701(table)_Part1Short
# after it has been joined with part2. Saved from access as Delimited and Comma separator, tick keep headings
# This table is normally saved to ValExtractDataOnly2017Val2
# in future need to look at this step and redo it to avoid using access.DONE!

# end dres needs to be parsed or cahnged to format d-m-y, otherwise values of df_FelixAgentMMDD_Table['END'] are incorrect. 
#Change later

#  call from another file df_FelixAgentMMDD(table) = FelixAgentYYMMTable(), now just reading csv

import numpy as np
from datetime import datetime
# from FelixAgent_query import FelixAgentYYMMTable
import os


# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'

PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')

print DATA
print RESULT


# defining functions
def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan

def catch_err_date(df, list_names):
	for item in list_names:
		if type(df.ix[0,item]) == str:
			# df.ix[:,item] = [x.replace(' 0:00:00', '') for x  in df.ix[:,item]]
			try:
				df[item] = pd.to_datetime(df[item],  format='%d-%m-%Y', dayfirst = True, errors = 'raise')
			except ValueError as e:
				print(e)
			try:
				df[item] = pd.to_datetime(df[item],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')
			except ValueError as e:
				print(e)
		df[item] = pd.to_datetime(df[item],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')
	return df

# Reading Files
df_FelixRiskwithLife = pd.read_csv(os.path.join(PROJ, 'output-files') + '\\FelixRisk(table)frompyodbc.csv',
		usecols = ['POL_NO', 'policy_id', 'dres', 'endd', 'stat', 'statd'], 
		dtype = {'POL_NO':np.int32, 'policy_id':np.int32, 'stat':np.int32, 'dres':str})

# df_FelixRiskwithLife = pd.read_csv(DATA + '\\FelixRiskwithLifeNoBenNo@20170701(table)_Part1Shortv3.csv',
# 				usecols = ['POL_NO', 'policy_id', 'dres', 'endd', 'stat', 'statd'],
# 				dtype = {'stat':np.int32, 'dres':str},
# 				skipinitialspace=True)

df_FelixAgentMMDD_Table = pd.read_csv(RESULT +'\\FelixAgentYYMM(table).csv') 

print df_FelixRiskwithLife[['dres', 'endd']].head(25)
print type(df_FelixRiskwithLife['statd'][0])
print type(df_FelixRiskwithLife['endd'][0])

df_FelixRiskwithLife = catch_err_date(df_FelixRiskwithLife, ['endd', 'statd'])

print df_FelixRiskwithLife[['statd', 'endd']].head(25)
print type(df_FelixRiskwithLife['statd'][0])

# print df_FelixAgentMMDD_Table['BEN_EFF'].unique() BEN_EFF is already in the format needed
					
df_FelixAgentMMDD_Table = catch_err_date(df_FelixAgentMMDD_Table, ['BEN_EFF', 'OUT_DAT'])

df_FelixAgentMMDD_Table['PolLifeNo'] = np.nan

df_FelixAgentMMDD_Table.rename(columns = {'AGT_1_NO':'Agent1No' , 
					'SubAgent1No':'AdjSubAgent1no', 'AGT_1_PC':'Agent1No%', 
					'AGT_2_NO':'Agent2No', 'SubAgent2No':'AdjSubAgent2No'
					 })
	
df_FelixAgentMMDD_Table=pd.merge(df_FelixAgentMMDD_Table, df_FelixRiskwithLife[['POL_NO', 'stat', 'statd', 'dres', 'endd']],
						on = 'POL_NO', how = 'left' )
			# df = pd.merge(df,df2[['Key_Column','Target_Column1', 'Target_Column2']],on='Key_Column', how='left')



df_FelixAgentMMDD_Table['Latest_OUT_DAT-temp'] = np.where(df_FelixAgentMMDD_Table['statd'].isnull(), 
													df_FelixAgentMMDD_Table['OUT_DAT'],
													df_FelixAgentMMDD_Table['statd'])

df_FelixAgentMMDD_Table['Latest_OUT_DAT-temp'] = df_FelixAgentMMDD_Table['Latest_OUT_DAT-temp'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else '')

# # boolean mask
df_FelixAgentMMDD_Table['Latest_OUT_DAT'] = np.where(df_FelixAgentMMDD_Table['stat'].isin(['1.0', '2.0', '6.0']), 
													np.nan,
													df_FelixAgentMMDD_Table['Latest_OUT_DAT-temp'])

print df_FelixAgentMMDD_Table[['Latest_OUT_DAT', 'PolicyNo', 'stat', 'statd']][0:5]


df_FelixAgentMMDD_Table['Latest_MOVT_CAUSE-temp'] = np.where(df_FelixAgentMMDD_Table['stat'] == 7,
										'LAP',
										np.where(df_FelixAgentMMDD_Table['stat'] == 5,
											'EXP',
											np.where(df_FelixAgentMMDD_Table['stat'] == 4, 
														'REP', 
														np.where(df_FelixAgentMMDD_Table['stat'] == 3,
																'CAN',
																df_FelixAgentMMDD_Table['MOVT_CAUSE']))))

# type is str, but had some whitespaces, some None: removing all whitespaces, replacing ''(etc) as np.nan
df_FelixAgentMMDD_Table['dres'] = df_FelixAgentMMDD_Table['dres'].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('', np.nan)

df_FelixAgentMMDD_Table['Latest_MOVT_CAUSE'] = np.where(df_FelixAgentMMDD_Table['stat'].isin(['1', '2', '6']),
												np.nan,
												np.where(df_FelixAgentMMDD_Table['dres'].isnull(),
															df_FelixAgentMMDD_Table['Latest_MOVT_CAUSE-temp'],
															df_FelixAgentMMDD_Table['dres']))



# print df_FelixAgentMMDD_Table[['stat','MOVT_CAUSE', 'dres','Latest_MOVT_CAUSE']].head(50)


df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE-temp'] = np.where(df_FelixAgentMMDD_Table['endd'].isnull(), 
												df_FelixAgentMMDD_Table['BEN_EFF'],
												df_FelixAgentMMDD_Table['endd'])
df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE-temp'] = df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE-temp'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else '') 
# # # boolean mask
df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE'] = np.where(df_FelixAgentMMDD_Table['stat'].isin(['1', '2', '6']),
												np.nan,
												df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE-temp'])


# print df_FelixAgentMMDD_Table[['stat', 'endd', 'BEN_EFF', 'Latest_MOVT_EFF_DATE']]



df_FelixAgentMMDD_Table.rename(columns = {'AGT_1_NO':'AGENT_1_NO', 'AGT_2_NO':'AGENT_2_NO',
											'Agent1Name':'AGENT-NAME',
											'SubAgent1No': 'AdjSubAgent1no', 'SubAgent2No':'AdjSubAgent2no',
											'SubAgent1Name':'SUBAGENT-NAME'}, inplace = True)

df_FelixAgentMMDD_Table[[	'A_Basic_Prem', 'A_Extra_Prem', 'DBEN', 'MASTER-NO', 'ENDValn']] =\
	df_FelixAgentMMDD_Table[['BASIC_PREM', 'EXTRA_PREM', 'DOE_BEN', 'AGENT_1_NO', 'VALN_DATE']]

df_FelixAgentMMDD_Table[['Agent1No', 'Agent1No%', 'Agent2No']] =\
	df_FelixAgentMMDD_Table[['AGENT_1_NO', 'AGENT_1_PC', 'AGENT_2_NO']]

df_FelixAgentMMDD_Table[['Opt_1_Ben', 'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben',  'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben']] =\
	df_FelixAgentMMDD_Table[['OPT_1_PREM', 'OPT_2_PREM', 'OPT_3_PREM', 'OPT_4_PREM', 'OPT_5_PREM', 'OPT_6_PREM', 'OPT_7_PREM']]
	


df_FelixAgentMMDD_Table['AdjSTAMP_DUTY'] = np.where(df_FelixAgentMMDD_Table['STAMP_DUTY'] > df_FelixAgentMMDD_Table['OAP'], 
										df_FelixAgentMMDD_Table['OAP'], 
										df_FelixAgentMMDD_Table['STAMP_DUTY'])

df_FelixAgentMMDD_Table['AdjPOL_FEE'] = np.where(df_FelixAgentMMDD_Table['BASIC_PREM'] > df_FelixAgentMMDD_Table['POL_FEE'], 
										df_FelixAgentMMDD_Table['POL_FEE'], 
										df_FelixAgentMMDD_Table['BASIC_PREM'])

print df_FelixAgentMMDD_Table[['STAMP_DUTY', 'OAP', 'AdjSTAMP_DUTY','BASIC_PREM', 'POL_FEE', 'AdjPOL_FEE']][0:5]



df_FelixAgentMMDD_Table['END'] = np.where(df_FelixAgentMMDD_Table['Latest_MOVT_CAUSE'].isnull(),
										df_FelixAgentMMDD_Table['ENDValn'],
										np.where(df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE'] < df_FelixAgentMMDD_Table['ENDValn'], 
											df_FelixAgentMMDD_Table['Latest_MOVT_EFF_DATE'],
											df_FelixAgentMMDD_Table['ENDValn']))

print df_FelixAgentMMDD_Table[['BEN_EFF','endd', 'Latest_MOVT_EFF_DATE','Latest_MOVT_CAUSE','ENDValn','END']].head(15)



df_FelixAgentMMDD_Table['A_Stamp_Duty'] = np.where(df_FelixAgentMMDD_Table['STAMP_DUTY'] > df_FelixAgentMMDD_Table ['OAP'], 
													df_FelixAgentMMDD_Table['OAP'], df_FelixAgentMMDD_Table['STAMP_DUTY'])
df_FelixAgentMMDD_Table['A_Pol_Fee'] = np.where(df_FelixAgentMMDD_Table['BASIC_PREM'] > df_FelixAgentMMDD_Table['POL_FEE'], 
													df_FelixAgentMMDD_Table['POL_FEE'], df_FelixAgentMMDD_Table['BASIC_PREM'])

df_FelixAgentMMDD_Table['ProdType'] = np.where(
				df_FelixAgentMMDD_Table['PROD_CLASS'].isin(['LII', 'BEX', 'LEX', 'ISS','ISP', 'ISC', 'ISR', 'DII', 'SI5']),
					'DII',
					'LS')

col = ['PolicyNo', 'POL_NO', 'STAT_FUND', 'PROD_CODE', 'PROD_CLASS', 'APP_TABLE',
 'PROD_ORIGN', 'REGISTER', 'STAT_POL', 'STAT_SI', 'STAT_SP', 'STAT_AP', 'SEX',
 'DOB', 'DOE_POL', 'AGE_RATING', 'BAS_LDG', 'PMILLE_LDG', 'OPT_LDG', 'SMK_STATUS',
 'OCC_CLASS', 'BRO_NO', 'FOUND_IND', 'DOE_BEN', 'ALT_DATE', 'TERM_BEN',
 'INS_TYPE', 'SI', 'PUSI', 'COMM_INI', 'COMM_REN', 'BEN_PERIOD', 'WAIT_PEROD',
 'PAY_STATUS', 'BASIC_PREM', 'BASIC_TERM', 'EXTRA_PREM', 'EXTRA_TERM',
 'OPT_1_CODE', 'OPT_1_PREM', 'OPT_1_TERM', 'OPT_2_CODE', 'OPT_2_PREM',
 'OPT_2_TERM', 'OPT_3_CODE', 'OPT_3_PREM','OPT_3_TERM', 'OPT_4_CODE',
 'OPT_4_PREM', 'OPT_4_TERM', 'OPT_5_CODE', 'OPT_5_PREM', 'OPT_5_TERM',
 'OPT_6_CODE', 'OPT_6_PREM', 'OPT_6_TERM', 'OPT_7_CODE', 'OPT_7_PREM',
 'OPT_7_TERM', 'STAMP_DUTY', 'OAP', 'OAP_INS', 'PAY_FREQ', 'DUE_MONTH',
 'REIN_TYPE', 'REIN_COY', 'REIN_PREM', 'REIN_TERM', 'REIN_SI', 'REIN_RET',
 'ANN_DATE', 'RERATE_IND', 'INDEX_IND', 'PREM_IND', 'DISC_IND', 'POL_FEE',
 'PERSIST_MA', 'PERSIST_AG', 'IO_LDG', 'PO_LDG', 'TOTAL_SI', 'LAST_SI',
 'LAST_TOTAL', 'IF_IND', 'MOVT_CAUSE', 'OUT_DAT', 'LAST_PAY', 'LAST_DATE',
 'COMM_IND', 'COMM_REBAT', 'BEN_EFF', 'BIUF_DTH', 'BIUF_TPD', 'POL_EFF',
 'MDLD_IND', 'SIMP_IND', 'CONT_IND', 'PREM_TYPE', 'BLC_IND', 'AGENT_1_PC',
 'AGENT_1_NO', 'AGENT_2_NO', 'COMM_CAT', 'COMM_TYPE', 'DUMPKEY', 'VALN_DATE',
 'VALN_YEAR', 'VALN_MONTH', 'ADJ_BEN', 'AGE_NEXT', 'AGE_NEAR', 'DURN',
 'START_YEAR', 'ANN_MONTH', 'PROD_TYPE', 'OLD_TABLE',  'PROD_CODE1',
 'PRODUCT', 'GROUP', 'RE_TPD_PR', 'RE_TPD_SI', 'RE_TRM_PR', 'RE_TRM_SI',
 'POST_CODE', 'DDFNAME', 'RECORDNO', 'PolLifeNo', 'Agent1No', 'AdjSubAgent1no',
 'Agent1No%', 'Agent2No', 'AdjSubAgent2no', 'Latest_OUT_DAT',
 'Latest_MOVT_CAUSE', 'Latest_MOVT_EFF_DATE', 'A_Basic_Prem', 'A_Extra_Prem',
 'ENDValn', 'END', 'DBEN', 'AdjSTAMP_DUTY', 'AdjPOL_FEE', 'Opt_1_Ben',
 'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben',
 'A_Pol_Fee', 'A_Stamp_Duty', 'MASTER-NO', 'AGENT-NAME', 'SUBAGENT-NAME',
 'First Name', 'Last Name', 'ProdType']

df_FelixAgentMMDD_Table = df_FelixAgentMMDD_Table[col].sort_values(by = 'POL_NO')

df_FelixAgentMMDD_Table.to_csv(RESULT + '\\RiskYYAllAgentsBenEffDateYYYYmonthNo3mhtsDelay(Felix)Table.csv',
								index = False)
print ('End')
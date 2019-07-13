#reads YYYYMonthStartingNo3mnthDelayPolList for previous year 
#FelixRisk, LapDate,FelixAgent for current period year 1707

import pandas as pd
import numpy as np
from datetime import datetime
import os


# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'

file_name = '\\YYYYMStartingPolListFelix_Felix(table).csv'
# file_name = '\\YYYYMStartingPolListFelix_WOP(table).csv'


print file_name[27:]


PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')



def catch_err_date(df, list_names):
	for item in list_names:
		if type(df.ix[0,item]) == str:
			df.ix[:,item] = [x.replace(' 0:00:00', '') for x  in df.ix[:,item]]
			try:
				df[item] = pd.to_datetime(df[item],  format='%d/%m/%Y', dayfirst = True, errors = 'raise')
			except ValueError as e:
				print(e)

			try:
				df[item] = pd.to_datetime(df[item],  format='%Y/%m/%d', dayfirst = True, errors = 'raise')
			except ValueError as e:
				print(e)

		df[item] = pd.to_datetime(df[item],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')
	return df




df_YearEnding = pd.read_csv(RESULT + file_name,
				usecols =['DOE_BEN', 'PolicyNo', 'POL_NO',
				'Latest_MOVT_CAUSE', 'Latest_MOVT_EFF_DATE', 
				'Latest_OUT_DAT', 'MOVT_CAUSE', 'Agent1No', 'AdjSubAgent1no', '1%', 'Agent2No', 
				'AdjSubAgent2no', '2%', 'SI', 'A_Basic_Prem', 'FOUND_IND', 'A_Extra_Prem', 
				'Opt_1_Ben', 'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben', 
				'A_Pol_Fee','A_Stamp_Duty', 'PROD_CLASS', 'Agent1Name', 'SubAgent1Name', 'Agent2Name', 
				'SubAgent2Name', 'First Name', 'Last Name', 'DOB', 'POST_CODE', 'ProdType', 'OUT_DAT', 
				'MOVT_CAUSE', 'BEN_EFF'],
				skipinitialspace=True)

df_FelixRiskwithLife = pd.read_csv(os.path.join(PROJ, 'output-files') + '\\FelixRisk(table)frompyodbc.csv',
				usecols = ['POL_NO', 'policy_id', 'dres', 'endd', 'stat', 'statd'],
				dtype = {'stat':np.int32, 'dres':str},
				skipinitialspace=True)

df_FelixAgent = pd.read_csv(RESULT +'\\FelixAgentYYMM(table).csv')

df_LapDate = pd.read_csv(os.path.join(DATA, 'MPE files') +\
	   '\\LapDate' + DCS_VAL_DATE + '.txt',
	   dtype = {'OLD-TABLE-CODE':np.int32, 'SUM-INSURED':np.int32,
	   'STATUS':str, 'CAUSE':str})


df_LapDate = catch_err_date(df_LapDate, ['STATUS-DATED', 'OFF-STATUS-DATE', 'EFFECTIVE-DATED'])
df_LapDate.rename(columns = {'POLICY-LIFE-BEN':'POL_NO', 'OLD-TABLE-CODE':'OLD_TABLE',
	'SUM-INSURED':'SI', 'STATUS':'STATUS', 'STATUS-DATED':'OUT_DAT',
	'CAUSE':'MOVT_CAUSE', 'EFFECTIVE-DATED':'MOVT_EFF_DATE'}, inplace = True)
print df_LapDate.info()


df_YearEnding = pd.merge(df_YearEnding,df_FelixRiskwithLife[['POL_NO','endd', 'stat', 'statd', 'dres']],
							on ='POL_NO', how = 'left')

df_YearEnding = pd.merge(df_YearEnding,df_LapDate[['POL_NO', 'STATUS', 'OUT_DAT', 'MOVT_EFF_DATE']],
							on ='POL_NO', how = 'left')

df_YearEnding = pd.merge(df_YearEnding,df_FelixAgent[['POL_NO', 'OUT_DAT', 'MOVT_CAUSE', 'BEN_EFF']],
 							on ='POL_NO', how = 'left',  suffixes = ('','-FelixAgent'))

print df_YearEnding.info()


df_YearEnding['New_Latest_OUT_DAT']= np.where(
				np.logical_or(df_YearEnding['STATUS'] == 'IF', df_YearEnding['stat'].isin(['1', '2', '6'])), 
		np.nan,
		np.where((df_YearEnding['OUT_DAT'].isnull() & df_YearEnding['statd'].isnull()), 
				df_YearEnding['Latest_OUT_DAT'], 
				np.where(df_YearEnding['OUT_DAT'].isnull(), 
					df_YearEnding['statd'], 
					df_YearEnding['OUT_DAT'])))


print df_YearEnding['stat'].head(2)
print df_YearEnding.loc[0,'stat']
print type(df_YearEnding.loc[0,'stat'])
print df_YearEnding['stat'] == 1

print df_YearEnding['OUT_DAT'].unique()
# df_YearEnding['OUT_DAT'] = df_YearEnding['OUT_DAT'].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isnull(x) else '')

print df_YearEnding.head(20)

df_YearEnding['New_Latest_ MOVT_CAUSE'] = np.where(
	np.logical_or(df_YearEnding['STATUS'] == 'IF', df_YearEnding['stat'].isin(['1', '2', '6'])),
		np.nan, 
		np.where(df_YearEnding['MOVT_CAUSE'].isnull(),
			np.where(df_YearEnding['dres'].isnull(),
				np.where(df_YearEnding['stat'] == 7,
						'LAP',
						np.where(df_YearEnding['stat'] == 5,
							'EXP', 
							np.where(df_YearEnding['stat'] == 4,
								'REP', 
								np.where(df_YearEnding['stat'] == 3,
									'CAN', 
									df_YearEnding['dres'])))),
				df_YearEnding['dres']),
			df_YearEnding['MOVT_CAUSE']))

df_YearEnding['New_Latest_MOVT_EFF_DATE'] = np.where(np.logical_or(df_YearEnding['STATUS'] == 'IF', df_YearEnding['stat'].isin(['1', '2', '6'])),
		np.nan, 
		np.where( (df_YearEnding['MOVT_EFF_DATE'].isnull()) & (df_YearEnding['endd'].isnull()),
			df_YearEnding['Latest_MOVT_EFF_DATE'], 
			np.where(df_YearEnding['MOVT_EFF_DATE'].isnull(), 
				df_YearEnding['endd'], 
				df_YearEnding['MOVT_EFF_DATE'])))
			
df_YearEnding['PolCnt'] = np.where(df_YearEnding['FOUND_IND'] == 'Y', 1, 0)

df_YearEnding.to_csv(RESULT + '\\YYYYMEndingNo3mnthDelayPolList_' + file_name[27:], index = False)
print ('Completed')


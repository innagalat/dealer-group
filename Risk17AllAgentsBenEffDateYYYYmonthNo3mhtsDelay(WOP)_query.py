# risk1707-unionQuery to be pasted to this code to the Risk17AllAgentsBeneffDate..(WOP)_Query


# checked, except of "do-it", all good


# PREM_IND, 
# A_Basic_Prem
# A_Extra_Prem
# AdjPOL_FEE
# OPT_...



# Stat_SI wrong
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os


# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'

PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')

# defining functions
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


def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan


def opt_ben_calc(term, prem):
	df_RiskYYMMQuery['OPT_TERM_0check'] = np.where(df_RiskYYMMQuery[term].isnull(), 
													
													df_RiskYYMMQuery['TERM_BEN_99check'],
													np.where(
																np.logical_and( 
																				df_RiskYYMMQuery[term] > df_RiskYYMMQuery['BASIC_TERM'],
																				df_RiskYYMMQuery['OLD_TABLE-stat801'] == False),
																			
																						df_RiskYYMMQuery['BASIC_TERM'],
																						df_RiskYYMMQuery[term]))


	df_RiskYYMMQuery['DBENvsEND-OPTind'] = np.where(df_RiskYYMMQuery.apply(lambda row: row['DOE_BEN'] + relativedelta(years = row['OPT_TERM_0check']),
																							axis =1) > df_RiskYYMMQuery['END'], 1, 0)
	
	return df_RiskYYMMQuery['DBENvsEND-OPTind'] * df_RiskYYMMQuery[prem]


def polfeeadj(ann_month, df_column):
	if ann_month > 2:
		pol_fee = pol_fee_up.get(df_column, df_column)
	else: pol_fee = pol_fee_down.get(df_column, df_column)
	return pol_fee

# _____________________________________

# reading files

df_RiskYYMMQuery = pd.read_csv(RESULT + '\\RiskYYMMQuery(table).csv', skipinitialspace=True)
df_LapDate = pd.read_csv(os.path.join(DATA, 'MPE files') +\
	   '\\LapDate' + DCS_VAL_DATE + '.txt',
	   dtype = {'OLD-TABLE-CODE':np.int32, 'SUM-INSURED':np.int32,
	   'STATUS':str, 'CAUSE':str})
df_PolAgentNo =  pd.read_csv( os.path.join(DATA, 'MPE files') +\
	   '\\PolAgtNo' + DCS_VAL_DATE + '.txt', 
	   dtype = {'POLICY-NO':np.int32, 'AGENT1-NO':np.int32, 'SUB-AGENT1-NO':np.int32,
	   'AGENT1-PERC':np.float64, 'AGENT2-NO':np.int32, 'SUB-AGENT2-NO':np.int32,
	   'COMM-REBATE-PERC':np.float64, 'COMM-SERVICE':np.int32, 
	   'POLICY-TABLE':str, 'STATUS':str, 'CAUSE':str, 'COMM-BASIS':str, 'COMM-CATEGORY':str,
	   'COMM-TYPE':str})				
df_AGTK =  pd.read_csv(os.path.join(DATA, 'MPE files') +\
		   '\\AGTKSAM' + DCS_VAL_DATE + '.txt',
		   dtype = {'MASTER-NO':np.int32, 'AGENT-NO':np.int32, 
		   'COMPANY':str, 'MASTER-STATUS':str, 'AGENT-TYPE':str, 'STATUS':str,
		   'CLASSIFICATION':str, 'ACCOUNT-NAME':str, 'AGENT-NAME':str})
df_BeneficiariesType =  pd.read_csv(os.path.join(DATA, 'MPE files') +\
	   '\\BeneficiariesType@' + DCS_VAL_DATE + '.txt',
	   dtype = {'POLICY-NO':np.int32, 'LIFE-NO':np.int32, 'SUM-INSURED':np.int32,
	   'SEX':str, 'NAME':str, 'OTHER-NAMES':str, 'OWNERS-NAME':str, 'BFC-TYPE':str})



print df_RiskYYMMQuery[['DOE_BEN','VALN_DATE']].loc[df_RiskYYMMQuery['POL_NO'] == 47310970101]
print type(df_RiskYYMMQuery.ix[0,'DOE_BEN'])



df_RiskYYMMQuery['VALN_DATE'] = pd.to_datetime(df_RiskYYMMQuery['VALN_DATE'],  format='%Y%m%d', dayfirst = True, errors = 'raise')
df_RiskYYMMQuery['DOE_BEN'] = pd.to_datetime(df_RiskYYMMQuery['DOE_BEN'],  format='%Y%m%d', dayfirst = True, errors = 'raise')


df_RiskYYMMQuery['TERM_BEN_99check'] = np.where(df_RiskYYMMQuery['TERM_BEN'].isnull(), 
												99, 
												df_RiskYYMMQuery['TERM_BEN']
												)

df_RiskYYMMQuery['BASIC_TERM_0check'] = np.where(df_RiskYYMMQuery['BASIC_TERM'].isnull(), 
												df_RiskYYMMQuery['TERM_BEN_99check'],
												df_RiskYYMMQuery['BASIC_TERM']
												)


# doesn't work correctly
# df_RiskYYMMQuery = catch_err_date(df_RiskYYMMQuery, ['DOE_BEN', 'VALN_DATE'])


# ____________________________________________________________________

df_LapDate.rename(columns = {'POLICY-LIFE-BEN':'POL_NO', 'OLD-TABLE-CODE':'OLD_TABLE',
	'SUM-INSURED':'SI', 'STATUS':'STATUS', 'STATUS-DATED':'OUT_DAT',
	'CAUSE':'MOVT_CAUSE', 'EFFECTIVE-DATED':'MOVT_EFF_DATE'}, inplace = True)

df_LapDate = catch_err_date(df_LapDate, ['OUT_DAT', 'OFF-STATUS-DATE', 'MOVT_EFF_DATE'])


# ________________________________________________


df_PolAgentNo.rename(columns = {'POLICY-NO':'PolicyNo', 'POLICY-TABLE':'PolStatus',
	'STATUS':'PolStatus', 'STATUSDATE':'PolStatusDate', 'CAUSE':'MovementCause',
	'AGENT1-NO':'Agent1', 'SUB-AGENT1-NO':'SubAgent1', 'AGENT1-PERC':'SubAgent1%',
	'AGENT2-NO':'Agent2', 'SUB-AGENT2-NO':'SubAgent2', 'COMM-BASIS':'CommBasis',
	'COMM-CATEGORY':'CommCategory', 'COMM-REBATE-PERC':'CommRebate%',
	'COMM-SERVICE':'CommService', 'COMM-TYPE':'CommType'}, inplace = True)

df_PolAgentNo = catch_err_date(df_PolAgentNo, ['PolStatusDate'])

# ______________________________________________



# renaming for merging
df_AGTK['Agent1']= df_AGTK['AGENT-NO']
df_AGTK['SubAgent1']= df_AGTK['AGENT-NO']
df_PolAgentNo = pd.merge(df_PolAgentNo, df_AGTK[['Agent1', 'MASTER-NO', 'AGENT-NAME']], on ='Agent1', how = 'left')
df_PolAgentNo = pd.merge(df_PolAgentNo, df_AGTK[['SubAgent1', 'AGENT-NAME']],
												on ='SubAgent1', how = 'left', suffixes =('_Agent1', '_SubAgent1'))
df_PolAgentNo['AdjSubAgent1no'] = np.where(df_PolAgentNo['SubAgent1'] == 0, 
										df_PolAgentNo['Agent1'], 
										df_PolAgentNo['SubAgent1'])
df_PolAgentNo['AdjSubAgent2no'] = np.where(df_PolAgentNo['SubAgent2'] == 0, 
										df_PolAgentNo['Agent2'], 
										df_PolAgentNo['SubAgent2'])
df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_PolAgentNo[['PolicyNo', 'MASTER-NO', 'AGENT-NAME_Agent1',
															'AGENT-NAME_SubAgent1', 'Agent1', 'SubAgent1%', 'Agent2']], 
															on ='PolicyNo', how = 'left')
df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_PolAgentNo[['PolicyNo','AdjSubAgent1no','AdjSubAgent2no']], on = 'PolicyNo', how = 'left')
df_RiskYYMMQuery.rename(columns = {'AGENT-NAME_Agent1': 'AGENT-NAME', 'AGENT-NAME_SubAgent1': 'SUBAGENT-NAME',
									'Agent1':'Agent1No', 'SubAgent1%':'Agent1No%', 'Agent2':'Agent2No'}, inplace = True)


# _______________________________________________________________



			# df = pd.merge(df,df2[['Key_Column','Target_Column1', 'Target_Column2']],on='Key_Column', how='left')

# ____________________________

df_LapDate['OUT_DAT'] = df_LapDate['OUT_DAT'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else '')
df_LapDate['Latest_OUT_DAT'] = np.where(df_LapDate['STATUS'] == 'IF', 
									np.nan, 
									df_LapDate['OUT_DAT'])

if True in df_LapDate['OUT_DAT'].isnull().unique():
	df_LapDate['OUT_DAT-lapdate'] = df_LapDate['OUT_DAT']
	df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_LapDate[['POL_NO', 'OUT_DAT-lapdate', 'Latest_OUT_DAT']], on = 'POL_NO', how = 'left')
	df_RiskYYMMQuery['Latest_OUT_DAT'] = np.where(df_RiskYYMMQuery['OUT_DAT-lapdate'].isnull(), 
										df_RiskYYMMQuery['OUT_DAT'], 
										df_RiskYYMMQuery['Latest_OUT_DAT'])
else: df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_LapDate[['POL_NO', 'Latest_OUT_DAT']], on = 'POL_NO', how = 'left')



df_LapDate['MOVT_CAUSE'] = df_LapDate['MOVT_CAUSE'].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('', np.nan)
df_LapDate['Latest_MOVT_CAUSE'] = np.where(df_LapDate['STATUS'] == 'IF', 
								np.nan, 
								df_LapDate['MOVT_CAUSE'])


if True in df_LapDate['MOVT_CAUSE'].isnull().unique():
	df_LapDate['MOVT_CAUSE-lapdate'] = df_LapDate['MOVT_CAUSE']
	# print df_LapDate[['STATUS', 'MOVT_CAUSE', 'Latest_MOVT_CAUSE']].loc[df_LapDate['Latest_MOVT_CAUSE'].isnull()].head(20)
	df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_LapDate[['POL_NO', 'MOVT_CAUSE-lapdate', 'Latest_MOVT_CAUSE']], on = 'POL_NO', how = 'left')
	df_RiskYYMMQuery['MOVT_CAUSE'] = df_RiskYYMMQuery['MOVT_CAUSE'].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('', np.nan)
	df_RiskYYMMQuery['Latest_MOVT_CAUSE'] = np.where(df_RiskYYMMQuery['MOVT_CAUSE-lapdate'].isnull(), 
									df_RiskYYMMQuery['MOVT_CAUSE'], 
									df_RiskYYMMQuery['Latest_MOVT_CAUSE'])
	# print df_RiskYYMMQuery[['MOVT_CAUSE', 'Latest_MOVT_CAUSE', 'MOVT_CAUSE-lapdate']].loc[df_RiskYYMMQuery['MOVT_CAUSE-lapdate'].isnull()].head(20)
else: df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_LapDate[['POL_NO', 'Latest_MOVT_CAUSE']], on = 'POL_NO', how = 'left')



df_LapDate['Latest_MOVT_EFF_DATE'] = np.where(df_LapDate['STATUS'] == 'IF', 
										np.nan,
										df_LapDate['MOVT_EFF_DATE'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else ''))
df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_LapDate[['POL_NO', 'MOVT_EFF_DATE', 'Latest_MOVT_EFF_DATE']], on = 'POL_NO', how = 'left')

if True in df_LapDate['MOVT_EFF_DATE'].isnull().unique():
	df_RiskYYMMQuery['Latest_MOVT_EFF_DATE'] = np.where(df_RiskYYMMQuery['MOVT_EFF_DATE'].isnull(), 
											df_RiskYYMMQuery['OUT_DAT'], 
											df_RiskYYMMQuery['Latest_MOVT_EFF_DATE'])


# df_RiskYYMMQuery.rename(columns = {'DOE_BEN':'DBEN'}, inplace = True) 
# remove repetition
df_RiskYYMMQuery['DBEN'] = df_RiskYYMMQuery['DOE_BEN']


df_RiskYYMMQuery['ENDValn'] = np.where(df_RiskYYMMQuery['DDFNAME'].isin(['term9910.ddf', 'dii9910.ddf']), 
								pd.to_datetime('2000/01/01',  format='%Y/%m/%d', dayfirst = True, errors = 'raise'), 
								df_RiskYYMMQuery['VALN_DATE'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else ''))

df_RiskYYMMQuery['ENDValn'] = pd.to_datetime(df_RiskYYMMQuery['ENDValn'],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')
df_RiskYYMMQuery['ENDValn'] = df_RiskYYMMQuery['ENDValn'].apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else '')

df_RiskYYMMQuery['END'] = np.where(df_RiskYYMMQuery['Latest_MOVT_CAUSE'].isnull(), 
									df_RiskYYMMQuery['ENDValn'], 
									np.where(df_RiskYYMMQuery['Latest_MOVT_EFF_DATE'] < df_RiskYYMMQuery['ENDValn'],
										df_RiskYYMMQuery['Latest_MOVT_EFF_DATE'],
										df_RiskYYMMQuery['ENDValn']))
df_RiskYYMMQuery['END'] = pd.to_datetime(df_RiskYYMMQuery['END'],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')

# additional variable
if type(df_RiskYYMMQuery.ix[0,'OLD_TABLE'])==np.int64:
	df_RiskYYMMQuery['OLD_TABLE-stat801'] = np.where(df_RiskYYMMQuery['OLD_TABLE'] == 801, True, False)
else:print ('do smth with type')
 

pol_fee_up = {  57:57.9,
				59:59.3,
				60:60.85, 
				62:62.55,
				64:64.6,
				66:66.5,
				68:68.95,
				70:70.35,
				72:72.2,
				74:74.4,
				76:76,
				78:78.05,
				
			}
pol_fee_down = {57:56.2,
				59:57.9,
				60:59.3,
				62:60.85,
				64:62.55,
				66:64.6,
				68:66.5,
				70:68.95,
				72:70.35,
				74:72.2,
				76:74.4,
				78:76
			}

print df_RiskYYMMQuery['POL_FEE'].unique()
print df_RiskYYMMQuery.ix[0:5,'POL_FEE']



# print df_RiskYYMMQuery.ix[0:5,['ANN_MONTH','POL_FEE']]
# for i in range(0,5):
# 	print polfeeadj(df_RiskYYMMQuery.ix[i,'ANN_MONTH'], df_RiskYYMMQuery.ix[i,'POL_FEE']), df_RiskYYMMQuery['PolLifeNo'][i]

# print df_RiskYYMMQuery.apply(lambda row: polfeeadj(row['ANN_MONTH'], row['POL_FEE']),
# 																						axis =1)[0:5]

df_RiskYYMMQuery['POL_FEE-table801'] = np.where(df_RiskYYMMQuery['OLD_TABLE-stat801'] == True, 
																df_RiskYYMMQuery['OPT_3_PREM'],
																df_RiskYYMMQuery['BASIC_PREM'])

df_RiskYYMMQuery['AdjPOL_FEE'] = np.where(
					df_RiskYYMMQuery['POL_FEE-table801'] > df_RiskYYMMQuery['POL_FEE'],
									df_RiskYYMMQuery.apply(lambda row: polfeeadj(row['ANN_MONTH'], row['POL_FEE']),
																						axis =1),
									df_RiskYYMMQuery['POL_FEE-table801'])


print df_RiskYYMMQuery[['PolicyNo','POL_FEE-table801','BASIC_PREM','AdjPOL_FEE', 'OPT_3_PREM']].loc[df_RiskYYMMQuery['POL_NO'] == 37698660101]




df_RiskYYMMQuery[ 'Opt_1_Ben'] = opt_ben_calc('OPT_1_TERM', 'OPT_1_PREM')
df_RiskYYMMQuery[ 'Opt_2_Ben'] = opt_ben_calc('OPT_2_TERM', 'OPT_2_PREM')
df_RiskYYMMQuery[ 'Opt_3_Ben'] = opt_ben_calc('OPT_3_TERM', 'OPT_3_PREM')
df_RiskYYMMQuery[ 'Opt_4_Ben'] = opt_ben_calc('OPT_4_TERM', 'OPT_4_PREM')
df_RiskYYMMQuery[ 'Opt_5_Ben'] = opt_ben_calc('OPT_5_TERM', 'OPT_5_PREM')
df_RiskYYMMQuery[ 'Opt_6_Ben'] = opt_ben_calc('OPT_6_TERM', 'OPT_6_PREM')
df_RiskYYMMQuery[ 'Opt_7_Ben'] = opt_ben_calc('OPT_7_TERM', 'OPT_7_PREM')

print df_RiskYYMMQuery[['POL_NO', 'Opt_1_Ben', 'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben']].loc[df_RiskYYMMQuery['POL_NO'] == 18850780101]


# _____________________________
df_RiskYYMMQuery['DBENvsEND-BASICind'] = np.where(df_RiskYYMMQuery.apply(lambda row: row['DOE_BEN'] + relativedelta(years = row['BASIC_TERM_0check']),
																						axis =1) > df_RiskYYMMQuery['END'], 1, 0)

df_RiskYYMMQuery['AdjSTAMP_DUTY'] = np.where(df_RiskYYMMQuery['STAMP_DUTY'] > df_RiskYYMMQuery['OAP'], 
										df_RiskYYMMQuery['OAP'], 
										df_RiskYYMMQuery['STAMP_DUTY'])

df_RiskYYMMQuery['BASIC_TERM_ADJPOL']=np.where(np.logical_or(df_RiskYYMMQuery['BASIC_TERM'] > 0,
										np.logical_and(df_RiskYYMMQuery['OLD_TABLE-stat801'],df_RiskYYMMQuery['OPT_3_TERM'] > 0)), 
										df_RiskYYMMQuery['AdjPOL_FEE'], 0)
df_RiskYYMMQuery['BASIC_TERM_ADJSD']=np.where(np.logical_or(df_RiskYYMMQuery['BASIC_TERM'] > 0,
										np.logical_and(df_RiskYYMMQuery['OLD_TABLE-stat801'],df_RiskYYMMQuery['OPT_3_TERM'] > 0)), 
										0, df_RiskYYMMQuery['AdjSTAMP_DUTY'])

df_RiskYYMMQuery['A_Pol_Fee'] = df_RiskYYMMQuery['BASIC_TERM_ADJPOL'] * df_RiskYYMMQuery['DBENvsEND-BASICind']
df_RiskYYMMQuery['A_Stamp_Duty'] = df_RiskYYMMQuery['BASIC_TERM_ADJSD'] * df_RiskYYMMQuery['DBENvsEND-BASICind']

print type(df_RiskYYMMQuery.ix[0,'END'])	


df_RiskYYMMQuery['A_Basic_Prem'] = df_RiskYYMMQuery['DBENvsEND-BASICind'] * df_RiskYYMMQuery['BASIC_PREM'] - np.where(df_RiskYYMMQuery['OLD_TABLE-stat801'],0,df_RiskYYMMQuery['A_Pol_Fee'])
print df_RiskYYMMQuery['A_Basic_Prem'].head(10)




# df_RiskYYMMQuery.apply(lambda row: row['DOE_BEN'] + relativedelta(years = row['BASIC_TERM_0check']),
# 																						axis =1).head(5)


print df_RiskYYMMQuery[['PolicyNo','A_Basic_Prem', 'END', 'DBEN', 'BASIC_PREM', 'A_Pol_Fee', 'AdjPOL_FEE']].head(5)


df_RiskYYMMQuery['A_Extra_Prem'] = 'to do'
# print df_RiskYYMMQuery[['DBEN', 'EXTRA_TERM','BASIC_TERM', 'TERM_BEN', 'EXTRA_PREM', 'END', 'BASIC_PREM', 'OLD_TABLE']].head(20)




# print df_RiskYYMMQuery[['EXTRA_TERM','BASIC_TERM']].loc[df_RiskYYMMQuery['EXTRA_TERM'] > df_RiskYYMMQuery['BASIC_TERM']].head(50)

# df_RiskYYMMQuery['NEW_DATE'] = df_RiskYYMMQuery['DBEN'].apply(lambda x: x + relativedelta(years=5))


df_RiskYYMMQuery['Years-temp'] = np.where(df_RiskYYMMQuery['EXTRA_TERM'] > df_RiskYYMMQuery['BASIC_TERM'], 
										0, 
										np.where(df_RiskYYMMQuery['EXTRA_TERM'] == 0, 
												df_RiskYYMMQuery['TERM_BEN_99check'],
												df_RiskYYMMQuery['EXTRA_TERM']))
print df_RiskYYMMQuery['Years-temp'].head(5)


# # 
# print type(df_RiskYYMMQuery.loc[0,'DBEN'].to_pydatetime())
# df_RiskYYMMQuery['DBEN'] = df_RiskYYMMQuery['DBEN'].apply(lambda x: x.to_pydatetime())

# df_RiskYYMMQuery['Offset-temp'] = df_RiskYYMMQuery['Years-temp'].apply(lambda x: relativedelta(years = x))
# print df_RiskYYMMQuery['DBEN'] + df_RiskYYMMQuery['Offset-temp']

# works, but can't figure out later step
# for i in range(0, df_RiskYYMMQuery.shape[0]):
# 	df_RiskYYMMQuery.loc[i,'DBEN-temp'] = df_RiskYYMMQuery.loc[i, 'DBEN'] + relativedelta(years = df_RiskYYMMQuery.loc[i, 'Years-temp'])


# df_RiskYYMMQuery['DBEN-temp'] = df_RiskYYMMQuery['DBEN'] + df_RiskYYMMQuery['Years-temp']
# print df_RiskYYMMQuery['DBEN-temp']

# df_RiskYYMMQuery['A_Extra_Prem'] = np.where(df_RiskYYMMQuery['DBEN-temp'] > df_RiskYYMMQuery['END'], 
# 											df_RiskYYMMQuery['EXTRA_PREM']*(-1), 0)

# # df_RiskYYMMQuery['Years'] = pd.to_datetime(df_RiskYYMMQuery['Years'],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')
# # df_RiskYYMMQuery['END'] = pd.to_datetime(df_RiskYYMMQuery['Years'],  format='%Y-%m-%d', dayfirst = True, errors = 'raise')



df_RiskYYMMQuery['ProdType'] = np.where(df_RiskYYMMQuery['PROD_CLASS'].isin(['LII', 'DII']), 
										'DII', 'LS')

# _____________________________________________________________


df_BeneficiariesType.rename(columns = {'POLICY-NO':'PolicyNo','LIFE-NO':'Life-No',
	'BIRTH-DATE':'DOB','SEX':'Sex', 'NAME':'Last Name', 'OTHER-NAMES':'First Name',
	'OWNERS-NAME':'Owners-Name', 'BFC-TYPE':'BFC-Type', 'BFC-EFFECTIVE-DA':'BFC-Eff-Date'},
						inplace = True)


df_BeneficiariesType['PolLifeNo'] = np.where(df_BeneficiariesType['Life-No'] < 10,
										df_BeneficiariesType.apply(lambda x:'%s%s%s'  %  (x['PolicyNo'],0, x['Life-No']),axis=1),
										df_BeneficiariesType.apply(lambda x:'%s%s'  %  (x['PolicyNo'],x['Life-No']),axis=1))
df_BeneficiariesType.ix[:,'PolLifeNo'] = [as_integer(x) for x in df_BeneficiariesType.ix[:, 'PolLifeNo']]



print ('__________________')



df_RiskYYMMQuery = pd.merge(df_RiskYYMMQuery, df_BeneficiariesType[['PolLifeNo', 'First Name', 'Last Name']], on ='PolLifeNo', how = 'left')
# doesn't work for some reason
# df_RiskYYMMQuery.ix[:,'First Name'] = [x.replace('  ', ' ') for x in df_RiskYYMMQuery.ix[:,'First Name']]
df_RiskYYMMQuery['First Name'] = df_RiskYYMMQuery['First Name'].apply(lambda x: x.strip() if isinstance(x, str) else x).replace('  ', ' ')

print df_RiskYYMMQuery.head(3)




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

 # 'START_MNTH',

df_RiskYYMMQuery= df_RiskYYMMQuery[col].sort_values(['PolicyNo', 'POL_NO'])
df_RiskYYMMQuery.to_csv(RESULT +'\\RiskYYAllAgentsBenEffDateYYYYmonthNo3mhtsDelay(WOP)Table.csv',
									index = False)
print ('script completed')
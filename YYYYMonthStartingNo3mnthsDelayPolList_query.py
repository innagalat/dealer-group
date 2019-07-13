import pandas as pd
import numpy as np
from datetime import datetime
import os

# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'

# file_name = '\\RiskYYAllAgentsBenEffDateYYYYmonthNo3mhtsDelay(Felix)Table.csv'
file_name = '\\RiskYYAllAgentsBenEffDateYYYYmonthNo3mhtsDelay(WOP)Table.csv'


PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')



file_name_res = file_name[48:-10]


df_Risk17 = pd.read_csv(RESULT + file_name)
				# usecols =['PolicyNo', 'POL_NO','Agent1No', 'Agent1No%', 'AdjSubAgent1no', 'Agent2No', 'AdjSubAgent2no', 'AGENT-NAME', 
				# 'Latest_MOVT_CAUSE', 'Latest_MOVT_EFF_DATE'],
				# skipinitialspace=True)

df_AGTK =  pd.read_csv(os.path.join(DATA, 'MPE files') +\
           '\\AGTKSAM' + DCS_VAL_DATE + '.txt',
	       dtype = {'MASTER-NO':np.int32, 'AGENT-NO':np.int32, 
	       'COMPANY':str, 'MASTER-STATUS':str, 'AGENT-TYPE':str, 'STATUS':str,
	       'CLASSIFICATION':str, 'ACCOUNT-NAME':str, 'AGENT-NAME':str})

DG_list =['2058792']


print df_AGTK.head(2)


df_AGTK['Agent1No'] = df_AGTK['AGENT-NO']
df_Risk17 = pd.merge(df_Risk17, df_AGTK[['Agent1No','MASTER-NO', 'AGENT-NAME']], on = 'Agent1No', how ='left', suffixes=('','-Agent1Name'))
	# df = pd.merge(df,df2[['Key_Column','Target_Column']],on='Key_Column', how='left')

print df_Risk17['MASTER-NO'].isin(DG_list).unique()
df_Risk17['1%'] = np.where(df_Risk17['MASTER-NO'].isin(DG_list), df_Risk17['Agent1No%'], 0)
df_Risk17['2%'] = np.where(df_Risk17['MASTER-NO'].isin(DG_list), 100 - df_Risk17['Agent1No%'], 0)
df_Risk17.rename(columns = {'AGENT-NAME-Agent1Name': 'Agent1Name'}, inplace = True)


df_AGTK['AdjSubAgent1no'] = df_AGTK['AGENT-NO']
df_Risk17 = pd.merge(df_Risk17, df_AGTK[['AdjSubAgent1no','AGENT-NAME']], on = 'AdjSubAgent1no', how ='left', suffixes=('','-SubAgent1Name'))
	# df = pd.merge(df,df2[['Key_Column','Target_Column']],on='Key_Column', how='left')
df_Risk17.rename(columns = {'AGENT-NAME-SubAgent1Name': 'SubAgent1Name'}, inplace = True)

df_AGTK['Agent2No'] = df_AGTK['AGENT-NO']
df_Risk17 = pd.merge(df_Risk17, df_AGTK[['Agent2No','MASTER-NO', 'AGENT-NAME']], on = 'Agent2No', how ='left', suffixes = ('','-2no'))
df_Risk17.rename(columns = {'MASTER-NO-2no': 'Master2No', 'AGENT-NAME-2no':'Agent2Name'}, inplace = True)


df_AGTK['AdjSubAgent2no'] = df_AGTK['AGENT-NO']
df_Risk17 = pd.merge(df_Risk17, df_AGTK[['AdjSubAgent2no','AGENT-NAME']], on = 'AdjSubAgent2no', how ='left', suffixes=('','-SubAgent2Name') )
df_Risk17.rename(columns = { 'AGENT-NAME-SubAgent2Name':'SubAgent2Name'}, inplace = True)


# print df_Risk17.loc[df_Risk17['PolicyNo'] == 1006336930]

# print df_Risk17.loc[df_Risk17['MASTER-NO'].isin(DG_list)] 
# print df_Risk17['Master2No'].isin(DG_list).unique()
# df_Risk17['MASTER-NO'].unique()
# print df_Risk17['Master2No'].unique()





df_Risk17['Latest_MOVT_EFF_DATE'] = pd.to_datetime(df_Risk17['Latest_MOVT_EFF_DATE'], format='%Y-%m-%d', dayfirst = True, errors = 'raise')
df_Risk17['Ind-temp'] = df_Risk17['Latest_MOVT_EFF_DATE'] > pd.to_datetime('20170630')



cond1 = np.logical_or(df_Risk17['MASTER-NO'].isin(DG_list), df_Risk17['Master2No'].isin(DG_list)) & df_Risk17['Latest_MOVT_CAUSE'].isnull()
cond2 = np.logical_or(df_Risk17['MASTER-NO'].isin(DG_list), df_Risk17['Master2No'].isin(DG_list)) &\
							df_Risk17['Latest_MOVT_CAUSE'].notnull() &\
							df_Risk17['Ind-temp'] == True
df_Risk17 = df_Risk17.loc[np.logical_or(cond1,cond2)]

print df_Risk17.shape[0]


col = ['PolicyNo', 'POL_NO', 'STAT_FUND', 'PROD_CODE', 'PROD_CLASS', 'APP_TABLE', 'PROD_ORIGN', 'REGISTER',
	'STAT_POL', 'STAT_SI', 'STAT_SP', 'STAT_AP', 'SEX', 'DOB', 'DOE_POL', 'AGE_RATING', 'BAS_LDG', 'PMILLE_LDG',
	'OPT_LDG', 'SMK_STATUS', 'OCC_CLASS', 'BRO_NO', 'FOUND_IND', 'DOE_BEN', 'ALT_DATE', 'TERM_BEN', 'INS_TYPE',
 	'SI', 'PUSI', 'COMM_INI', 'COMM_REN', 'BEN_PERIOD', 'WAIT_PEROD', 'PAY_STATUS', 'BASIC_PREM', 'BASIC_TERM',
  	'EXTRA_PREM', 'EXTRA_TERM', 'OPT_1_CODE', 'OPT_1_PREM', 'OPT_1_TERM', 'OPT_2_CODE', 'OPT_2_PREM', 'OPT_2_TERM',
   	'OPT_3_CODE', 'OPT_3_PREM', 'OPT_3_TERM', 'OPT_4_CODE', 'OPT_4_PREM', 'OPT_4_TERM', 'OPT_5_CODE', 'OPT_5_PREM',
    'OPT_5_TERM', 'OPT_6_CODE', 'OPT_6_PREM', 'OPT_6_TERM', 'OPT_7_CODE', 'OPT_7_PREM', 'OPT_7_TERM', 'STAMP_DUTY',
    'OAP', 'OAP_INS', 'PAY_FREQ', 'DUE_MONTH', 'REIN_TYPE', 'REIN_COY', 'REIN_PREM', 'REIN_TERM', 'REIN_SI',
    'REIN_RET', 'ANN_DATE', 'RERATE_IND', 'INDEX_IND', 'PREM_IND', 'DISC_IND', 'POL_FEE', 'PERSIST_MA',
    'PERSIST_AG', 'IO_LDG', 'PO_LDG', 'TOTAL_SI', 'LAST_SI', 'LAST_TOTAL', 'IF_IND', 'MOVT_CAUSE', 'OUT_DAT',
    'LAST_PAY', 'LAST_DATE', 'COMM_IND', 'COMM_REBAT', 'BEN_EFF', 'BIUF_DTH', 'BIUF_TPD', 'POL_EFF', 'MDLD_IND',
    'SIMP_IND', 'CONT_IND', 'PREM_TYPE', 'BLC_IND', 'AGENT_1_PC', 'AGENT_1_NO', 'AGENT_2_NO', 'COMM_CAT',
    'COMM_TYPE', 'DUMPKEY', 'VALN_DATE', 'VALN_YEAR', 'VALN_MONTH', 'ADJ_BEN', 'AGE_NEXT', 'AGE_NEAR',
    'DURN', 'START_YEAR', 'ANN_MONTH', 'PROD_TYPE', 'OLD_TABLE', 'PROD_CODE1', 'PRODUCT',
    'GROUP', 'RE_TPD_PR', 'RE_TPD_SI', 'RE_TRM_PR', 'RE_TRM_SI', 'POST_CODE', 'DDFNAME', 'RECORDNO',
    'PolLifeNo', 'Agent1No', 'AdjSubAgent1no', 'Agent1No%', 'Agent2No', 'AdjSubAgent2no', 'Latest_OUT_DAT',
    'Latest_MOVT_CAUSE', 'Latest_MOVT_EFF_DATE', 'A_Basic_Prem', 'A_Extra_Prem', 'ENDValn', 'END',
    'DBEN', 'AdjSTAMP_DUTY', 'AdjPOL_FEE', 'Opt_1_Ben', 'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben',
    'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben', 'A_Pol_Fee', 'A_Stamp_Duty', 'MASTER-NO', 'AGENT-NAME',
    'SUBAGENT-NAME', 'First Name', 'Last Name', 'ProdType', '1%', '2%', 'Agent1Name', 'SubAgent1Name', 'Master2No',
    'Agent2Name', 'SubAgent2Name']

df_Risk17= df_Risk17[col].sort_values(by = 'POL_NO')

df_Risk17.to_csv(RESULT + '\\YYYYMStartingPolListFelix_' + file_name_res + '(table).csv', index = False)

print file_name_res
print(RESULT + '\\YYYYMStartingPolListFelix_' + file_name_res + '(table).csv')
print ('End')

# removed 'START_MNTH' 


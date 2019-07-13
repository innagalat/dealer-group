# pasted this code to the FelixExtractMatchingPOL_NODouble_query, so this one no need to be used


# creating a FELIX AGENT TABLEa
# checked, except of "removed spaces" all good


import pandas as pd
import numpy as np
from datetime import datetime
import os
# from FelixExtractMatchingPol_NoDouble_query import FelixExtractMatchingPol_NoDouble_qry

DCS_VAL_DATE = '20180101'
folder = 'current period files'

# PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
# DATA = os.path.join(PROJ, 'data')
# RESULT = os.path.join(PROJ, 'output-files')


PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')



def FelixAgentYYMMTable():

	print ('Reading files')
	df_FelixExtractMatch = pd.read_csv(RESULT + '\\FelixExtractMatchingPol_NoDouble_interm.csv')
	# df_FelixExtractMatch = FelixExtractMatchingPol_NoDouble_qry()

	df_FelixRiskPepl = pd.read_csv(DATA +\
			'\\FelixRiskPepl(table)frompyodbc.csv')
	# df_FelixRiskPepl = pd.read_csv(DATA +\
	# 		'\\qry_stg_ftx_pepl_current.csv')
	
	
	df_AGTK =  pd.read_csv(os.path.join(DATA, 'MPE files') +\
	       '\\AGTKSAM' + DCS_VAL_DATE + '.txt',
	       dtype = {'MASTER-NO':np.int32, 'AGENT-NO':np.int32, 
	       'COMPANY':str, 'MASTER-STATUS':str, 'AGENT-TYPE':str, 'STATUS':str,
	       'CLASSIFICATION':str, 'ACCOUNT-NAME':str, 'AGENT-NAME':str})



	df_FelixExtractMatch['AGENT-NO'] = df_FelixExtractMatch['SubAgent1No']
	df = pd.merge(df_FelixExtractMatch, df_AGTK[['AGENT-NO','AGENT-NAME']],on='AGENT-NO', how='left')
	# df = pd.merge(df,df2[['Key_Column','Target_Column']],on='Key_Column', how='left')
	df.rename(columns ={'AGENT-NAME':'SubAgent1Name'}, inplace = True)



	df['AGENT-NO'] = df_FelixExtractMatch['SubAgent2No']
	df = pd.merge(df, df_AGTK[['AGENT-NO','AGENT-NAME']], on='AGENT-NO', how='left')
	df.rename(columns ={'AGENT-NAME':'SubAgent2Name'}, inplace = True)
	df.drop('AGENT-NO', axis= 1, inplace = True)


	df['id'] = df_FelixExtractMatch['L_LIFE_ID']
	print df['id'].head(6)
	df = pd.merge(df, df_FelixRiskPepl[['id', 'name1_mixed_case']], on = 'id', how = 'left')
	df = pd.merge(df, df_FelixRiskPepl[['id', 'name2_mixed_case']], on = 'id', how = 'left')
	df.rename(columns ={'name1_mixed_case':'First Name'}, inplace = True)
	df.rename(columns ={'name2_mixed_case':'Last Name'}, inplace = True)
	df.drop('id', axis= 1, inplace = True)

	return df

df = FelixAgentYYMMTable()
col = ['PolicyNo', 'POL_NO', 'L_LIFE_NO', 'B_BEN_NO', 'STAT_FUND', 'PROD_CODE',
	'PROD_CLASS', 'APP_TABLE', 'PROD_ORIGN', 'REGISTER', 'STAT_POL', 'STAT_SI',
	'STAT_SP', 'STAT_AP', 'SEX', 'DOB', 'DOE_POL', 'AGE_RATING', 'BAS_LDG',
	'PMILLE_LDG', 'OPT_LDG', 'SMK_STATUS', 'OCC_CLASS', 'BRO_NO', 'FOUND_IND',
	'DOE_BEN', 'ALT_DATE', 'TERM_BEN', 'INS_TYPE', 'SI', 'PUSI', 'COMM_INI',
	'COMM_REN', 'BEN_PERIOD', 'WAIT_PEROD', 'PAY_STATUS', 'BASIC_PREM',
	'BASIC_TERM', 'EXTRA_PREM', 'EXTRA_TERM', 'OPT_1_CODE', 'OPT_1_PREM',
	'OPT_1_TERM', 'OPT_2_CODE', 'OPT_2_PREM', 'OPT_2_TERM', 'OPT_3_CODE',
	'OPT_3_PREM', 'OPT_3_TERM', 'OPT_4_CODE', 'OPT_4_PREM', 'OPT_4_TERM',
	'OPT_5_CODE', 'OPT_5_PREM', 'OPT_5_TERM', 'OPT_6_CODE', 'OPT_6_PREM',
	'OPT_6_TERM', 'OPT_7_CODE', 'OPT_7_PREM', 'OPT_7_TERM', 'STAMP_DUTY', 'OAP',
	'OAP_INS', 'PAY_FREQ', 'DUE_MONTH', 'REIN_TYPE', 'REIN_COY', 'REIN_PREM',
	'REIN_TERM', 'REIN_SI', 'REIN_RET', 'ANN_DATE', 'RERATE_IND', 'INDEX_IND',
	'PREM_IND', 'DISC_IND', 'POL_FEE', 'PERSIST_MA', 'PERSIST_AG', 'IO_LDG',
	'PO_LDG', 'TOTAL_SI', 'LAST_SI', 'LAST_TOTAL', 'IF_IND', 'MOVT_CAUSE', 'OUT_DAT',
	'LAST_PAY', 'LAST_DATE', 'COMM_IND', 'COMM_REBAT', 'BEN_EFF', 'BIUF_DTH',
	'BIUF_TPD', 'POL_EFF', 'MDLD_IND', 'SIMP_IND', 'CONT_IND', 'PREM_TYPE',
	'BLC_IND', 'AGENT_1_PC', 'temp_AGENT_1_NO', 'temp_SubAgt1No', 'COMM_CAT',
	'COMM_TYPE', 'DUMPKEY', 'VALN_DATE', 'VALN_YEAR', 'VALN_MONTH', 'ADJ_BEN',
	'AGE_NEXT', 'AGE_NEAR', 'DURN', 'START_YEAR', 'ANN_MONTH', 'PROD_TYPE',
	'OLD_TABLE', 'START_MNTH', 'PROD_CODE1', 'PRODUCT', 'GROUP', 'RE_TPD_PR',
	'RE_TPD_SI', 'RE_TRM_PR', 'RE_TRM_SI', 'POST_CODE', 'DDFNAME', 'RECORDNO',
	'DCS_VAL_DATE', 'P_GP_TYPE', 'P_GP_CLASS', 'P_GP_ID', 'P_GP_STATUS',
	'P_GP_LIVES', 'DOXIT_POL', 'P_PREM_STAT', 'DOX_POL',
	'L_LIFE_ID', 'DOE_LIF', 'L_POL_FEE', 'DOX_BEN', 'B_COMP_KEY', 'B_LINK_SEQ',
	'B_SOU_OF_BUS', 'B_PAY_METH', 'B_CURR_SIA', 'B_CURR_SIAB', 'B_CURR_SIAC',
	'B_LAST_SIA', 'B_LAST_SIB', 'B_LAST_SIC', 'B_ORIG_SI', 'B_INSTAL_SI',
	'B_SI_ID_RATE', 'B_BASEPREMA', 'B_BASEPREMB', 'B_BASEPREMC', 'B_PREM_FRZE',
	'B_PAUSE_REM', 'B_ACCOPTPERC', 'B_W_DIS_LOAD', 'B_O_DEF_LOAD', 'B_OCC_LOAD',
	'B_PFEENEGDIC', 'B_RISK_DISC', 'B_EXP_DISC', 'B_SIZE_DISC', 'B_MULTBENDC',
	'B_POLPACKDC', 'B_GRPPACKDC', 'B_UWTRDOLOAD', 'B_FREQ_LOAD', 'B_SDIMP',
	'B_PREMCSDA', 'B_TAB_APREM', 'B_COMM_PREM', 'B_COMMPREMCPI', 'B_TERMTOMXEA',
	'B_COMMDDTYPE', 'B_COMREPNBAMT', 'B_COMREDREAS', 'B_COMREDPERC', 
	'B_COMBNNOPERC', 'B_COMBNRENPERC', 'B_COMBNENDDATE', 'B_COMBNFLATAMT',
	'B_COMOVERNBPERC', 'B_COMOVERRENPERC', 'B_AG1LICENSE', 'B_AG1COMSCALE',
	'B_AG2COMCAT', 'B_AG2LICENSE', 'B_AG2COMSCALE', 'B_AG2SHAPE', 'B_AG2SPLITPERC',
	'B_PTYP_AG2', 'B_RE1_ID', 'B_RE1_TYPE', 'B_RE1ORIGSI', 'B_RE1SUMRET',
	'B_RE1FRDLAR', 'B_RE1TODLAR', 'B_RE2_ID', 'B_RE2_TYPE', 'B_RE2_COMP',
	'B_RE2_TERM', 'B_RE2_SI', 'B_RE2ORIGSI', 'B_RE2SUMRET', 'B_RE2FRDLAR',
	'B_RE2TODLAR', 'B_RE_EX_DTLS', 'AGT_1_NO', 'SubAgent1No', 'AGT_1_PC',
	'AGT_2_NO', 'SubAgent2No', 'AGT_2_PC', 'Agent1Name', 'Agent2Name', 'B_SI_INDX',
	'B_EAPP_VERSION_NO', 'B_LOYALTY_MBR_NUMBER', 'B_LOYALTY_PGM_CD_TYPE',
	'B_LOYALTY_POINTS_PER_TERM', 'B_PAYMENT_METHOD', 'B_APP_TYPE', 'B_BB_OPT',
	'SubAgent1Name', 'SubAgent2Name', 'First Name', 'Last Name']

df= df[col]
print df.shape[0]

df.to_csv(RESULT +'\\FelixAgentYYMM(table).csv', index = False)


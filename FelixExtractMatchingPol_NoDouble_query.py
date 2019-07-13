# 4/01 - combined 2 files to one: FelixExtractMatchingPol_NoDouble_query and FelixAgent_Query

# creating a FELIX AGENT TABLEa
# checked, except of "removed spaces" all good


folder = 'current period files'

import pandas as pd
import numpy as np
from datetime import datetime
import os



# print ('please enter date in format 20170701')

# DCS_VAL_DATE = raw_input()
# print DCS_VAL_DATE

# DCS_VAL_DATE = '20171001'
# folder = 'previous quarter files'

DCS_VAL_DATE = '20180101'
folder = 'current period files'

print 'AGTKSAM'+DCS_VAL_DATE+'.txt'




PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
RESULT = os.path.join(DATA, 'output-files')

print DATA
print RESULT

# Defining functions
def replace_zeroto_nan(df, col_list):
	for i in col_list:
			df[i] = df[i].replace(0, np.nan)
	return df

def comb_dates(df, col1, col2, col3):
	replace_zeroto_nan(df, [col1, col2, col3])
	return pd.to_datetime((df[col1]*10000 + df[col2]*100 + df[col3]).apply(str),format='%Y%m%d')
#need to resolve an issue, where for pol  1000528698  'B_COMBNENDDAY'=99999, which can't be translated to year

def make_null(df, list_col):
	for i in list_col:
			df[i] = None
	return df

def make_zero(df, list_col):
	for i in list_col:
			df[i] = 0
	return df	



# Reading files
#______________________________________________
# df = pd.read_csv(os.path.join(PROJ, 'from access checking') +\
# 	   '\\FelixExtractMatchingPol_NoDouble.csv')

# print df.columns.values

df_FelixLifeRisk = pd.read_table(DATA +
		'\\OneCare_Data_Details.PRO', skiprows = [0,1,2], sep = ',', skipinitialspace = True)
		# nrows = 10)
print ('Reading files')


# removes spaces between words
for i in df_FelixLifeRisk.columns.values:
	if type(df_FelixLifeRisk.ix[0,i]) == str:
		df_FelixLifeRisk.ix[:,i] = [x.replace(' ', '') for x in df_FelixLifeRisk.ix[:,i]]


# df_FelixLifeRisk = df_FelixLifeRisk.ix[0:100,:]


df_FelixExtractMatch = pd.DataFrame()
# df_FelixExtractMatch = pd.DataFrame(index=np.arange(0, df_FelixLifeRisk.shape[0]), columns = df_FelixLifeRisk.columns.values)

df_FelixExtractMatch['PolicyNo'] = df_FelixLifeRisk['P_POL_NO']
df_FelixExtractMatch['POL_NO'] = df_FelixLifeRisk['P_POL_NO']*10000 +\
									df_FelixLifeRisk['L_LIFE_NO']*100 +\
									df_FelixLifeRisk['B_BEN_NO']

df_FelixExtractMatch[['L_LIFE_NO', 'B_BEN_NO', 'STAT_FUND', 'PROD_CLASS','PROD_ORIGN', 'REGISTER', 'STAT_SI', 'SEX']] =\
		df_FelixLifeRisk[['L_LIFE_NO', 'B_BEN_NO', 'P_FUND_ID', 'B_BEN_TYPE','P_HERITAGE', 'P_STATE', 'B_IF_SI', 'B_SEX']]

df_FelixExtractMatch['PROD_CODE'] = 'CINPAL'
df_FelixExtractMatch['APP_TABLE'] = None
df_FelixExtractMatch['BRO_NO'] = None
df_FelixExtractMatch['ALT_DATE'] = None
df_FelixExtractMatch['STAT_SP'] = 0
df_FelixExtractMatch['OPT_LDG'] = 0


df_FelixExtractMatch['STAT_POL'] = np.where(df_FelixLifeRisk['B_IF_IND'] == 1, 0, df_FelixLifeRisk['P_POL_CNT'])
df_FelixExtractMatch['STAT_AP'] = np.where(df_FelixLifeRisk['B_IF_IND'] == 1, 0, df_FelixLifeRisk['B_APREM'])


	# check
	# print df_FelixLifeRisk.ix[0:2, 'B_DOB_YEAR']
	# print df_FelixLifeRisk.ix[0:2, 'B_DOB_MTH']
	# print df_FelixLifeRisk.ix[0:2, 'B_DOB_DAY']

df_FelixExtractMatch['DOB'] = comb_dates(df_FelixLifeRisk, 'B_DOB_YEAR', 'B_DOB_MTH', 'B_DOB_DAY')
df_FelixExtractMatch['DOE_POL'] = comb_dates(df_FelixLifeRisk, 'P_RCD_YEAR', 'P_RCD_MTH','P_RCD_DAY')

df_FelixExtractMatch[['AGE_RATING', 'BAS_LDG', 'PMILLE_LDG', 'SMK_STATUS', 'OCC_CLASS']] =\
		df_FelixLifeRisk[['B_RATED_AGE', 'B_UWTR_LOAD', 'B_UWTRDOLOAD', 'B_SMOK', 'B_OCC_CLASS']]


# FOUND_IND
condition1 = (df_FelixLifeRisk['L_LIFE_NO'] > 1) & (df_FelixLifeRisk['B_BEN_NO'] == 1)
condition2 = (df_FelixLifeRisk['L_LIFE_NO'] == 1) & (df_FelixLifeRisk['B_BEN_NO'] == 1) & (df_FelixLifeRisk['P_IF_IND'] == 1)

df_FelixExtractMatch['FOUND_IND'] = np.where(np.logical_or(df_FelixLifeRisk['P_POL_CNT'] == 1, condition2),
											'Y',
											np.where(condition1, 'L', 'N'))



df_FelixExtractMatch['DOE_BEN'] = comb_dates(df_FelixLifeRisk, 'B_ENT_YEAR', 'B_ENT_MTH','B_ENT_DAY')

df_FelixExtractMatch[['TERM_BEN']] = df_FelixLifeRisk[['B_BEN_TERM']]

# INS_TYPE

df_FelixLifeRisk['INS_1st-temp'] = np.where(df_FelixLifeRisk['B_COMP_KEYC'].isnull(), '', df_FelixLifeRisk['B_COMP_KEYC'])
# print df_FelixLifeRisk['INS_1st']
df_FelixLifeRisk['INS_2st-temp'] = np.where(df_FelixLifeRisk['B_COMP_KEYC'].isnull(), '', df_FelixLifeRisk['B_COMP_KEYC'])
df_FelixExtractMatch['INS_TYPE'] = 'do-it'


#SI
df_FelixLifeRisk['SI_1st-temp'] = df_FelixLifeRisk['B_IF_SI']
df_FelixLifeRisk.loc[df_FelixLifeRisk['SI_1st-temp'] == 0, 'SI_1st-temp'] = df_FelixLifeRisk['B_ORIG_SI']

df_FelixLifeRisk['SI'] = df_FelixLifeRisk['B_CURR_SIA'] + df_FelixLifeRisk['B_CURR_SIAB'] +\
							df_FelixLifeRisk['B_CURR_SIAC']						
df_FelixLifeRisk.loc[df_FelixLifeRisk['SI'] == 0, 'SI'] = df_FelixLifeRisk['SI_1st-temp']
df_FelixLifeRisk.drop('SI_1st-temp', axis=1, inplace=True)

df_FelixExtractMatch[['SI', 'BEN_PERIOD', 'WAIT_PEROD', 'PAY_STATUS']] =\
		df_FelixLifeRisk[['SI', 'B_BEN_PERD', 'B_WAIT_PERD', 'B_PREM_STAT']]


df_FelixExtractMatch['OPT_1_CODE'] = np.where(df_FelixLifeRisk['B_BEN_TYPE'].isin(['TCC', 'TCP']),
																df_FelixLifeRisk['B_BB_OPT'], np.nan)
df_FelixExtractMatch['OPT_2_CODE'] = np.where(df_FelixLifeRisk['B_ACCOPTPERC'] != 0, 'Y', np.nan)
df_FelixExtractMatch['OPT_3-temp'] =np.where(df_FelixLifeRisk['B_BEN_TYPE'].isin(['TCP']), 'P', np.nan)
df_FelixExtractMatch['OPT_3_CODE'] = np.where(df_FelixLifeRisk['B_BEN_TYPE'].isin(['TCC']),'C', df_FelixExtractMatch['OPT_3-temp'])
df_FelixExtractMatch.drop('OPT_3-temp', axis = 1, inplace = True)
df_FelixExtractMatch['OPT_6_CODE'] = np.where(df_FelixLifeRisk['B_BEN_TYPE'].isin(['TPN', 'TPW']),
																df_FelixLifeRisk['B_BB_OPT'], np.nan)


# print df_FelixExtractMatch[['OPT_1_CODE', 'OPT_2_CODE', 'OPT_3_CODE', 'OPT_6_CODE']]
		
zero_col = ['PUSI','EXTRA_PREM', 'IO_LDG', 'PO_LDG', 'LAST_TOTAL', 'OPT_1_PREM',
				'OPT_2_PREM', 'OPT_3_PREM', 'OPT_4_PREM', 'OPT_5_PREM', 'OPT_6_PREM', 
				'OPT_7_PREM' , 'OAP_INS']

null_col = ['COMM_INI', 'COMM_REN',
				'BASIC_TERM', 'EXTRA_TERM', 'OPT_1_TERM',
				'OPT_2_TERM', 'OPT_3_TERM', 'OPT_4_TERM', 'OPT_5_TERM', 'OPT_6_TERM',
				'OPT_7_CODE', 'OPT_7_TERM', 'RERATE_IND' ]

make_zero(df_FelixExtractMatch, zero_col)
make_null(df_FelixExtractMatch, null_col)

df_FelixExtractMatch[['OPT_4_CODE', 'OPT_5_CODE', 'OAP',			'PAY_FREQ', 'DUE_MONTH', 'REIN_TYPE', 'REIN_COY']] =\
	df_FelixLifeRisk[['B_BEN_INDEX', 'B_SUB_TYPE', 'B_OFF_APREM','P_PREM_FREQ', 'P_DUE_MONTH', 'B_RE1_TYPE', 'B_RE1_COMP']]


	# PAY_FREQ: P_PREM_FREQ
	# DUE_MONTH: P_DUE_MONTH
	# REIN_TYPE: B_RE1_TYPE
	# REIN_COY: B_RE1_COMP

df_FelixExtractMatch[['REIN_PREM', 'REIN_TERM', 'REIN_SI', 'REIN_RET', 'INDEX_IND']] =\
		df_FelixLifeRisk[['B_RE1_PREM', 'B_RE1_TERM', 'B_RE1_SI', 'B_RE1SUMRET', 'B_SI_INDX']]

	# REIN_PREM: B_RE1_PREM
	# REIN_TERM: B_RE1_TERM
	# REIN_SI: B_RE1_SI
	# REIN_RET: B_RE1SUMRET
	# INDEX_IND: B_SI_INDX



	# print type(df_FelixLifeRisk.ix[72, 'P_NEXTANNYR'])
	# print df_FelixLifeRisk.ix[72, 'P_NEXTANNYR']

	# df_FelixLifeRisk['P_NEXTANNYR'] = df_FelixLifeRisk['P_NEXTANNYR'].replace(0, np.nan)

	# print df_FelixLifeRisk.ix[71:75, 'P_NEXTANNYR']



df_FelixExtractMatch['ANN_DATE'] = comb_dates(df_FelixLifeRisk, 'P_NEXTANNYR', 'P_NEXTANNMTH', 'P_NEXTANNDAY')


df_FelixExtractMatch['POL_FEE'] = df_FelixLifeRisk['B_POL_FEE'] - df_FelixLifeRisk['B_SDFEEEXP']



df_FelixExtractMatch['STAMP_DUTY'] = df_FelixLifeRisk['B_SDFEEEXP'] + df_FelixLifeRisk['B_SD_PREMEXP']

df_FelixExtractMatch['BASIC_PREM'] = df_FelixExtractMatch['OAP'] -\
				df_FelixExtractMatch['POL_FEE'] - df_FelixExtractMatch['STAMP_DUTY']



zero_col = ['IO_LDG', 'PO_LDG','LAST_TOTAL', 'LAST_PAY', 'LAST_DATE']
null_col = ['RERATE_IND', 'PREM_IND', 'DISC_IND', 'PERSIST_MA', 'PERSIST_AG']

make_zero(df_FelixExtractMatch, zero_col)
make_null(df_FelixExtractMatch, null_col)

df_FelixExtractMatch['TOTAL_SI']= df_FelixLifeRisk['L_TOT_IF_SI'] + df_FelixLifeRisk['L_TOIFINCCOV']
df_FelixExtractMatch['LAST_SI']= df_FelixLifeRisk['L_LASTOIFSI'] + df_FelixLifeRisk['L_LAIFINCCOV']


df_FelixExtractMatch[['REIN_PREM', 'REIN_TERM', 'REIN_SI', 'REIN_RET', 'INDEX_IND']] =\
		df_FelixLifeRisk[['B_RE1_PREM', 'B_RE1_TERM', 'B_RE1_SI', 'B_RE1SUMRET', 'B_SI_INDX']]

df_FelixExtractMatch['OUT_DAT'] = comb_dates(df_FelixLifeRisk, 'B_EXIT_YEAR', 'B_EXIT_MTH', 'B_EXIT_DAY')
df_FelixExtractMatch['BEN_EFF'] = comb_dates(df_FelixLifeRisk, 'B_EXITEFFYR', 'B_EXITEFFMTH', 'B_EXITEFFDAY')
	# OUT_DAT: [B_EXIT_YEAR]*10000+[B_EXIT_MTH]*100+[B_EXIT_DAY]
	# BEN_EFF: [b_EXITEFFYR]*10000+[b_EXITEFFMTH]*100+[b_EXITEFFDAY]

df_FelixExtractMatch[['IF_IND', 'MOVT_CAUSE', 'COMM_IND', 'COMM_REBAT', 'BIUF_DTH']] =\
		df_FelixLifeRisk[['B_IF_IND', 'B_EXIT_CAUSE', 'B_PTYP_AG1', 'B_COMDDPERC', 'B_BGLDPERC']]

df_FelixExtractMatch['BIUF_TPD'] = df_FelixExtractMatch['BIUF_DTH']
	# IF_IND: B_IF_IND
	# MOVT_CAUSE: B_EXIT_CAUSE
	# COMM_IND: B_PTYP_AG1
	# COMM_REBAT: B_COMDDPERC
	# BIUF_DTH: B_BGLDPERC
	# BIUF_TPD: B_BGLDPERC same value
	

df_FelixExtractMatch[['MDLD_IND', 'PREM_TYPE', 'AGENT_1_PC', 'COMM_CAT', 'COMM_TYPE']] =\
		df_FelixLifeRisk[['B_MDLD_DISC', 'B_PREM_TYPE', 'B_AG1SPLITPERC', 'B_AG1COMCAT', 'B_AG1SHAPE']]

	# MDLD_IND: B_MDLD_DISC
	# PREM_TYPE: B_PREM_TYPE
	# AGENT_1_PC: B_AG1SPLITPERC
	# COMM_CAT: B_AG1COMCAT
	# COMM_TYPE: B_AG1SHAPE

null_col = ['POL_EFF', 'SIMP_IND', 'CONT_IND', 'BLC_IND', 'temp_AGENT_1_NO', 'temp_SubAgt1No', 'DUMPKEY']
make_null(df_FelixExtractMatch, null_col)

	# POL_EFF: Null
	# SIMP_IND: Null
	# CONT_IND: Null
	# BLC_IND: Null
	# temp_AGENT_1_NO: Null
	# temp_SubAgt1No: Null
	# DUMPKEY: Null

df_FelixExtractMatch['DCS_VAL_DATE'] = DCS_VAL_DATE
val_date = datetime(year=int(DCS_VAL_DATE[0:4]), month=int(DCS_VAL_DATE[4:6]), day=int(DCS_VAL_DATE[6:8]))
df_FelixExtractMatch['VALN_DATE'] = val_date
df_FelixExtractMatch['VALN_YEAR'] = int(DCS_VAL_DATE[0:4])
df_FelixExtractMatch['VALN_MONTH'] = int(DCS_VAL_DATE[4:6])

make_zero(df_FelixExtractMatch, ['ADJ_BEN', 'DURN'])

	# ADJ_BEN: 0
	# DURN: 0

df_FelixLifeRisk['B_ENT_DATE'] = comb_dates(df_FelixLifeRisk, 'B_ENT_YEAR', 'B_ENT_MTH', 'B_ENT_DAY')
df_FelixLifeRisk['B_DOB_DATE'] = comb_dates(df_FelixLifeRisk, 'B_DOB_YEAR', 'B_DOB_MTH', 'B_DOB_DAY')


((df_FelixLifeRisk['B_ENT_MTH'] == df_FelixLifeRisk['B_DOB_MTH']) & (df_FelixLifeRisk['B_ENT_DAY'] == df_FelixLifeRisk['B_DOB_DAY'])).unique()

	# print df_FelixLifeRisk[['B_ENT_DATE', 'B_DOB_DATE', 'Age_NB']]

	# [B_ENT_MTH]=[B_DOB_MTH] And [B_ENT_DAY] >= [B_DOB_DAY]

	# df_FelixExtractMatch['AGE_NEXT-temp1'] = 
df_FelixExtractMatch['AGE_NEXT'] = 'do-it'
df_FelixExtractMatch['AGE_NEAR'] = 'do-it'

	# df_FelixExtractMatch['AGE_NEXT'] = 2
	# df_FelixExtractMatch.ix[:,'AGE_NEAR'] = [int(x) for x in (df_FelixExtractMatch.ix[:,'AGE_NEXT'] - 0.5)]
	# print df_FelixExtractMatch[['AGE_NEXT', 'AGE_NEAR']]


df_FelixExtractMatch[['START_YEAR', 'ANN_MONTH', 'PROD_TYPE', 'OLD_TABLE', 'START_MNTH']] =\
		df_FelixLifeRisk[['B_ENT_YEAR', 'P_NEXTANNMTH', 'P_SUPER_ORD', 'P_PROD_CD', 'B_ENT_MTH']]

	# START_YEAR: B_ENT_YEAR
	# ANN_MONTH: P_NEXTANNMTH
	# PROD_TYPE: P_SUPER_ORD
	# OLD_TABLE: P_PROD_CD
	# START_MNTH: B_ENT_MTH

for i in ['PROD_CODE1', 'PRODUCT', 'GROUP']:
		 df_FelixExtractMatch[i] = 'CINPAL'

make_zero(df_FelixExtractMatch, ['RE_TPD_PR', 'RE_TPD_SI', 'RE_TRM_PR', 'RE_TRM_SI', 'RECORDNO'])

	# RE_TPD_PR: 0
	# RE_TPD_SI: 0
	# RE_TRM_PR: 0
	# RE_TRM_SI: 0
	# RECORDNO: 0

make_null(df_FelixExtractMatch, ['DDFNAME'])
	# DDFNAME: Null

print df_FelixLifeRisk['P_GP_TYPE'].head(5)

df_FelixExtractMatch[['POST_CODE', 'P_GP_TYPE', 'P_GP_CLASS', 'P_GP_ID', 'P_GP_STATUS', 'P_GP_LIVES']] =\
		df_FelixLifeRisk[['P_POST_CODE', 'P_GP_TYPE', 'P_GP_CLASS', 'P_GP_ID', 'P_GP_STATUS', 'P_GP_LIVES']]

	# POST_CODE: P_POST_CODE
	# P_GP_TYPE:P_GP_TYPE
	# P_GP_CLASS:P_GP_CLASS
	# P_GP_ID:P_GP_ID
	# P_GP_STATUS:P_GP_STATUS
	# P_GP_LIVES:P_GP_LIVES


for i in ['P_PREM_STAT', 'L_POL_FEE','L_LIFE_ID']:
		df_FelixExtractMatch[i] = df_FelixLifeRisk[i]

	# P_PREM_STAT:P_PREM_STAT
	# L_POL_FEE:L_POL_FEE
	# L_LIFE_IDtext: L_LIFE_ID
	# L_LIFE_ID: [L_LIFE_IDtext]*1

df_FelixExtractMatch['DOXIT_POL']= comb_dates(df_FelixLifeRisk,'P_EXITEFFYR', 'P_EXITEFFMTH', 'P_EXITEFFDAY') 
df_FelixExtractMatch['DOX_POL']= comb_dates(df_FelixLifeRisk,'P_EXP_YEAR', 'P_EXP_MTH', 'P_EXP_DAY')


	# DOX_POL: [P_exp_YEAR]*10000+[P_exp_MTH]*100+[P_exp_DAY]
	# DOX_BEN: [B_exp_YEAR]*10000+[B_exp_MTH]*100+[B_exp_DAY]

df_FelixExtractMatch['DOE_LIF']= comb_dates(df_FelixLifeRisk,'L_ENT_YEAR', 'L_ENT_MTH', 'L_ENT_DAY') 
df_FelixExtractMatch['DOX_BEN']= comb_dates(df_FelixLifeRisk,'B_EXP_YEAR', 'B_EXP_MTH', 'B_EXP_DAY') 

df_FelixExtractMatch['B_COMP_KEY'] = df_FelixLifeRisk.apply(
									lambda x:'%s%s%s' % (x['B_COMP_KEYA'], x['B_COMP_KEYB'], x['B_COMP_KEYC']),
									axis=1)
df_FelixExtractMatch['B_PREMCSDA']= comb_dates(df_FelixLifeRisk,'B_PREMCSYEAR', 'B_PREMCSMTH', 'B_PREMCSDAY')
	
	
	# df_FelixExtractMatch['B_COMBNENDDATE']= comb_dates(df_FelixLifeRisk,'B_COMBENENDYR', 'B_COMBNENDMTH', 'B_COMBNENDDAY') 

df_FelixLifeRisk = replace_zeroto_nan(df_FelixLifeRisk, ['B_COMBENENDYR', 'B_COMBNENDMTH', 'B_COMBNENDDAY'])
df_FelixExtractMatch['B_COMBNENDDATE'] = df_FelixLifeRisk['B_COMBENENDYR']*10000 +\
				df_FelixLifeRisk['B_COMBNENDMTH']*100 + df_FelixLifeRisk['B_COMBNENDDAY']





list_col =['B_LINK_SEQ','B_SOU_OF_BUS','B_PAY_METH', 
				'B_CURR_SIA','B_CURR_SIAB', 'B_CURR_SIAC',
				'B_LAST_SIA', 'B_LAST_SIB', 'B_LAST_SIC', 
				'B_ORIG_SI', 'B_INSTAL_SI', 'B_SI_ID_RATE', 
				'B_BASEPREMA', 'B_BASEPREMB', 'B_BASEPREMC',
				'B_PREM_FRZE', 'B_PAUSE_REM', 'B_ACCOPTPERC', 
				'B_W_DIS_LOAD', 'B_O_DEF_LOAD', 'B_OCC_LOAD', 
				'B_PFEENEGDIC','B_RISK_DISC', 'B_EXP_DISC',
				'B_SIZE_DISC', 'B_MULTBENDC', 'B_POLPACKDC',
				'B_GRPPACKDC', 'B_UWTRDOLOAD', 'B_FREQ_LOAD']

for i in list_col:
		df_FelixExtractMatch[i] = df_FelixLifeRisk[i]

df_FelixExtractMatch['B_SDIMP'] = df_FelixLifeRisk['B_SDPREMIMP'] + df_FelixLifeRisk['B_SDFEEIMP']

	# B_PREMCSDA
list_col =['B_TAB_APREM', 'B_COMM_PREM', 'B_COMMPREMCPI', 'B_TERMTOMXEA', 'B_COMMDDTYPE',
		'B_COMREPNBAMT', 'B_COMREPNBAMT', 'B_TERMTOMXEA', 'B_COMMDDTYPE', 'B_COMREPNBAMT', 
		'B_COMREDREAS', 'B_COMREDPERC', 'B_COMBNNOPERC', 'B_COMBNRENPERC', 'B_COMBNFLATAMT',
		'B_COMOVERNBPERC', 'B_COMOVERRENPERC', 'B_AG1LICENSE', 
		'B_AG1COMSCALE', 'B_AG2COMCAT', 'B_AG2LICENSE', 'B_AG2COMSCALE', 
		'B_AG2SHAPE', 'B_AG2SPLITPERC', 'B_PTYP_AG2', 
		'B_RE1_ID', 'B_RE1_TYPE', 'B_RE1ORIGSI', 'B_RE1SUMRET',
		'B_RE1FRDLAR', 'B_RE1TODLAR', 'B_RE2_ID', 
		'B_RE2_TYPE', 'B_RE2_COMP', 'B_RE2_TERM', 'B_RE2_SI',
		'B_RE2ORIGSI', 'B_RE2SUMRET', 'B_RE2FRDLAR',
		'B_RE2TODLAR', 'B_RE_EX_DTLS']

for i in list_col:
		df_FelixExtractMatch[i] = df_FelixLifeRisk[i]


df_FelixExtractMatch[['AGT_1_NO', 'SubAgent1No', 'AGT_1_PC', 'AGT_2_NO', 'SubAgent2No']] =\
	 df_FelixLifeRisk[['B_AG1LIC_NO', 'B_AG1COM_ID', 'B_AG1SPLITPERC', 'B_AG2LIC_NO', 'B_AG2COM_ID']]

	# AGT_1_NO: B_AG1LIC_NO
	# SubAgent1No: B_AG1COM_ID
	# AGT_1_PC: B_AG1SPLITPERC
	# AGT_2_NO: B_AG2LIC_NO
	# SubAgent2No: B_AG2COM_ID

df_FelixExtractMatch[['AGT_2_PC', 'Agent1Name', 'Agent2Name']] =\
	 df_FelixLifeRisk[['B_AG2SPLITPERC', 'B_AG1LICENSE', 'B_AG2LICENSE']]

	# AGT_2_PC: B_AG2SPLITPERC
	# Agent1Name: B_AG1LICENSE
	# Agent2Name: B_AG2LICENSE


list_col = ['B_SI_INDX', 'B_EAPP_VERSION_NO', 'B_LOYALTY_MBR_NUMBER', 
			'B_LOYALTY_PGM_CD_TYPE', 'B_LOYALTY_POINTS_PER_TERM',
			'B_PAYMENT_METHOD', 'B_APP_TYPE','B_BB_OPT']

for i in list_col:
		df_FelixExtractMatch[i] = df_FelixLifeRisk[i]







# col = ['PolicyNo', 'POL_NO', 'L_LIFE_NO', 'B_BEN_NO', 'STAT_FUND', 'PROD_CODE',
#  'PROD_CLASS', 'APP_TABLE', 'PROD_ORIGN', 'REGISTER', 'STAT_POL', 'STAT_SI',
#  'STAT_SP', 'STAT_AP', 'SEX', 'DOB', 'DOE_POL', 'AGE_RATING', 'BAS_LDG',
#  'PMILLE_LDG', 'OPT_LDG', 'SMK_STATUS', 'OCC_CLASS', 'BRO_NO', 'FOUND_IND',
#  'DOE_BEN', 'ALT_DATE', 'TERM_BEN', 'INS_TYPE', 'SI', 'PUSI', 'COMM_INI',
#  'COMM_REN', 'BEN_PERIOD', 'WAIT_PEROD', 'PAY_STATUS', 'BASIC_PREM',
#  'BASIC_TERM', 'EXTRA_PREM', 'EXTRA_TERM', 'OPT_1_CODE', 'OPT_1_PREM',
#  'OPT_1_TERM', 'OPT_2_CODE', 'OPT_2_PREM', 'OPT_2_TERM', 'OPT_3_CODE',
#  'OPT_3_PREM', 'OPT_3_TERM', 'OPT_4_CODE', 'OPT_4_PREM', 'OPT_4_TERM',
#  'OPT_5_CODE', 'OPT_5_PREM', 'OPT_5_TERM', 'OPT_6_CODE', 'OPT_6_PREM',
#  'OPT_6_TERM', 'OPT_7_CODE', 'OPT_7_PREM', 'OPT_7_TERM', 'STAMP_DUTY', 'OAP',
#  'OAP_INS', 'PAY_FREQ', 'DUE_MONTH', 'REIN_TYPE', 'REIN_COY', 'REIN_PREM',
#  'REIN_TERM', 'REIN_SI', 'REIN_RET', 'ANN_DATE', 'RERATE_IND', 'INDEX_IND',
#  'PREM_IND', 'DISC_IND', 'POL_FEE', 'PERSIST_MA', 'PERSIST_AG', 'IO_LDG',
#  'PO_LDG', 'TOTAL_SI', 'LAST_SI', 'LAST_TOTAL', 'IF_IND', 'MOVT_CAUSE', 'OUT_DAT',
#  'LAST_PAY', 'LAST_DATE', 'COMM_IND', 'COMM_REBAT', 'BEN_EFF', 'BIUF_DTH', 'BIUF_TPD',
#  'POL_EFF', 'MDLD_IND', 'SIMP_IND', 'CONT_IND', 'PREM_TYPE',
#  'BLC_IND', 'AGENT_1_PC', 'temp_AGENT_1_NO', 'temp_SubAgt1No', 'COMM_CAT',
#  'COMM_TYPE', 'DUMPKEY', 'VALN_DATE', 'VALN_YEAR', 'VALN_MONTH', 'ADJ_BEN',
#  'AGE_NEXT', 'AGE_NEAR', 'DURN', 'START_YEAR', 'ANN_MONTH', 'PROD_TYPE',
#  'OLD_TABLE', 'START_MNTH', 'PROD_CODE1', 'PRODUCT', 'GROUP', 'RE_TPD_PR',
#  'RE_TPD_SI', 'RE_TRM_PR', 'RE_TRM_SI', 'POST_CODE', 'DDFNAME', 'RECORDNO',
#  'DCS_VAL_DATE', 'P_GP_TYPE', 'P_GP_CLASS', 'P_GP_ID', 'P_GP_STATUS',
#  'P_GP_LIVES', 'DOXIT_POL', 'P_PREM_STAT', 'DOX_POL',
#  'L_LIFE_ID', 'DOE_LIF', 'L_POL_FEE', 'DOX_BEN', 'B_COMP_KEY', 'B_LINK_SEQ',
#  'B_SOU_OF_BUS', 'B_PAY_METH', 'B_CURR_SIA', 'B_CURR_SIAB', 'B_CURR_SIAC',
#  'B_LAST_SIA', 'B_LAST_SIB', 'B_LAST_SIC', 'B_ORIG_SI', 'B_INSTAL_SI',
#  'B_SI_ID_RATE', 'B_BASEPREMA', 'B_BASEPREMB', 'B_BASEPREMC', 'B_PREM_FRZE',
#  'B_PAUSE_REM', 'B_ACCOPTPERC', 'B_W_DIS_LOAD', 'B_O_DEF_LOAD', 'B_OCC_LOAD',
#  'B_PFEENEGDIC', 'B_RISK_DISC', 'B_EXP_DISC','B_SIZE_DISC', 'B_MULTBENDC',
#  'B_POLPACKDC', 'B_GRPPACKDC', 'B_UWTRDOLOAD', 'B_FREQ_LOAD', 'B_SDIMP',
#  'B_PREMCSDA', 'B_TAB_APREM', 'B_COMM_PREM', 'B_COMMPREMCPI', 'B_TERMTOMXEA',
#  'B_COMMDDTYPE', 'B_COMREPNBAMT', 'B_COMREDREAS', 'B_COMREDPERC',
#  'B_COMBNNOPERC', 'B_COMBNRENPERC', 'B_COMBNENDDATE', 'B_COMBNFLATAMT',
#  'B_COMOVERNBPERC', 'B_COMOVERRENPERC', 'B_AG1LICENSE', 'B_AG1COMSCALE',
#  'B_AG2COMCAT', 'B_AG2LICENSE', 'B_AG2COMSCALE', 'B_AG2SHAPE', 'B_AG2SPLITPERC',
#  'B_PTYP_AG2', 'B_RE1_ID', 'B_RE1_TYPE', 'B_RE1ORIGSI', 'B_RE1SUMRET',
#  'B_RE1FRDLAR', 'B_RE1TODLAR', 'B_RE2_ID', 'B_RE2_TYPE', 'B_RE2_COMP',
#  'B_RE2_TERM', 'B_RE2_SI', 'B_RE2ORIGSI', 'B_RE2SUMRET', 'B_RE2FRDLAR',
#  'B_RE2TODLAR', 'B_RE_EX_DTLS', 'AGT_1_NO', 'SubAgent1No', 'AGT_1_PC',
#  'AGT_2_NO', 'SubAgent2No', 'AGT_2_PC', 'Agent1Name', 'Agent2Name', 'B_SI_INDX',
#  'B_EAPP_VERSION_NO', 'B_LOYALTY_MBR_NUMBER', 'B_LOYALTY_PGM_CD_TYPE',
#  'B_LOYALTY_POINTS_PER_TERM', 'B_PAYMENT_METHOD', 'B_APP_TYPE', 'B_BB_OPT']


 # not included  'AGE_NEAR' 'L_LIFE_IDtext'

# df_FelixExtractMatch = df_FelixExtractMatch[col]
# df_FelixExtractMatch.to_csv(RESULT + '\\FelixExtractMatchingPol_NoDouble_interm.csv', index = False)
print ('Completed, next step- create FelixAgent_query')

# _____________________________________________________
# Adding FelixAgent_query


df_FelixRiskPepl = pd.read_csv(os.path.join(PROJ, 'data') +\
			'\\FelixRiskPepl(table)frompyodbc.csv')
	# df_FelixRiskPepl = pd.read_csv(DATA +\
	# 		'\\qry_stg_ftx_pepl_current.csv')
	
	
df_AGTK =  pd.read_csv(os.path.join(DATA, 'MPE files') +\
	       '\\AGTKSAM'+DCS_VAL_DATE+'.txt',
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
df = pd.merge(df, df_FelixRiskPepl[['id', 'First Name']], on = 'id', how = 'left')
df = pd.merge(df, df_FelixRiskPepl[['id', 'Last Name']], on = 'id', how = 'left')
# df.rename(columns ={'name1_mixed_case':'First Name'}, inplace = True)
# df.rename(columns ={'name2_mixed_case':'Last Name'}, inplace = True)
df.drop('id', axis= 1, inplace = True)




print df.shape[0]

df.to_csv(RESULT +'\\FelixAgentYYMM(table).csv', index = False)


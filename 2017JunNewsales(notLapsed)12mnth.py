import pandas as pd
import numpy as np
from datetime import datetime
import os



DCS_VAL_DATE = '20180101'

folder_previous = 'previous quarter files'
# folder_previous = 'previous year files'


# file_name = '\\YYYYMStartingPolListFelix_Felix(table).csv'
file_name = '\\YYYYMStartingPolListFelix_WOP(table).csv'


PROJ = r'C:\\galati_files\\pyscripts\\DG Payments\\'
# DATA = os.path.join(PROJ, 'data\\proposed design\\' + folder)
# RESULT = os.path.join(DATA, 'output-files')


df_Starting =  pd.read_csv(os.path.join(PROJ, 'data\\proposed design\\current period files\\output-files\\') +\
				file_name,
				usecols =['DOE_BEN', 'PolicyNo', 'POL_NO', 'SI','Latest_OUT_DAT',
				'Latest_MOVT_CAUSE', 'Latest_MOVT_EFF_DATE', 
				'Agent1No', 'AdjSubAgent1no', '1%', 'Agent2No', 'AdjSubAgent2no', 
				'2%', 'FOUND_IND', 'A_Basic_Prem', 'A_Extra_Prem', 
				'Opt_1_Ben','Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben',
				'Opt_7_Ben', 'A_Pol_Fee',  'A_Stamp_Duty', 
				'DOB',
				'PROD_CLASS', 'Agent1Name', 'SubAgent1Name', 
				'Agent2Name', 'SubAgent2Name','First Name', 'Last Name', 'POST_CODE',
				'ProdType'],
				skipinitialspace=True) 

print df_Starting[df_Starting['Opt_6_Ben'].isnull() == True]

df_Ending_prev = pd.read_csv(PROJ + '\\data\\proposed design\\previous quarter files\\output-files\\' +\
				'YYYYMEndingNo3mnthDelayPolList' + file_name[26:],
				usecols = ['POL_NO','SI', 'A_Basic_Prem', 'A_Extra_Prem', 'Opt_1_Ben',
				'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben', 
				'A_Pol_Fee',  'A_Stamp_Duty'])


df_Starting.rename(columns = {'Latest_OUT_DAT':'New_Latest_OUT_DAT', 'Latest_MOVT_CAUSE':'New_Latest_MOVT_CAUSE',
					'Latest_MOVT_EFF_DATE':'New_Latest_MOVT_EFF_DATE', 

					# 'SI':'SI Beg', 
					# 'A_Basic_Prem': 'A_Basic_Prem Beg', 'A_Extra_Prem': 'A_Extra_Prem Beg',
					# 'Opt_1_Ben':,'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben',
					# 'Opt_7_Ben'

					}, inplace =True)

df_Starting['PolCnt'] = np.where(df_Starting['FOUND_IND'] == 'Y', 1, 0)


df_Starting = pd.merge(df_Starting,df_Ending_prev[['POL_NO','SI', 'A_Basic_Prem', 'A_Extra_Prem', 'Opt_1_Ben',
							'Opt_2_Ben', 'Opt_3_Ben', 'Opt_4_Ben', 'Opt_5_Ben', 'Opt_6_Ben', 'Opt_7_Ben', 
							'A_Pol_Fee',  'A_Stamp_Duty']],
							on ='POL_NO', how = 'left', suffixes = (' Beg', ' Ending'))


print df_Starting[df_Starting['Opt_6_Ben Ending'].isnull() == True]

exit(0)
# df_Starting[['POL_NO',
# 			'SI Beg', 'A_Basic_Prem Beg', 'A_Extra_Prem Beg', 'Opt_1_Ben Beg',
# 			'Opt_2_Ben Beg', 'Opt_3_Ben Beg', 'Opt_4_Ben Beg', 'Opt_5_Ben Beg', 'Opt_6_Ben Beg', 'Opt_7_Ben Beg', 
# 			'A_Pol_Fee Beg',  'A_Stamp_Duty Beg',
# 			'SI End', 'A_Basic_Prem End', 'A_Extra_Prem End', 'Opt_1_Ben End',
# 			'Opt_2_Ben End', 'Opt_3_Ben End', 'Opt_4_Ben End', 'Opt_5_Ben End', 'Opt_6_Ben End', 'Opt_7_Ben End', 
# 			'A_Pol_Fee End',  'A_Stamp_Duty End']].to_csv(os.path.join(PROJ, 'data\\proposed design\\current period files\\output-files\\') +'check.csv', index = False)

# print df_Starting['DOB'].head(8)



# This part is YYYMNewSalesPolListNo3mnthDelay(Not Lapsed)3mnthNewFinal
print('Specify Start of period ')
print DCS_VAL_DATE
df_Starting['StartOfPeriod'] = pd.to_datetime(DCS_VAL_DATE)



df_Starting['SI Start'] = np.where(df_Starting['SI Beg'].isnull(),
									0, df_Starting['SI Beg']* (df_Starting['1%'] + df_Starting['2%'])/100)

df_Starting['A_Extra_Prem Beg'] = 0
df_Starting['A_Basic_Prem Beg'] = 0
print df_Starting[['Opt_5_Ben Ending','Opt_6_Ben Ending']][720:723]
print df_Starting[df_Starting['Opt_6_Ben Ending'].isnull() == True]

df_Starting['OAP_start-temp'] = df_Starting['A_Basic_Prem Beg'] + df_Starting['A_Extra_Prem Beg'] +\
								df_Starting['Opt_1_Ben Beg'] + df_Starting['Opt_2_Ben Beg'] +\
								df_Starting['Opt_3_Ben Beg'] + df_Starting['Opt_4_Ben Beg'] +\
								df_Starting['Opt_5_Ben Beg'] + df_Starting['Opt_6_Ben Beg'] +\
								df_Starting['Opt_7_Ben Beg']

df_Starting['OAP_end-temp'] = df_Starting['A_Basic_Prem Ending'] + df_Starting['A_Extra_Prem Ending'] +\
								df_Starting['Opt_1_Ben Ending'] + df_Starting['Opt_2_Ben Ending'] +\
								df_Starting['Opt_3_Ben Ending'] + df_Starting['Opt_4_Ben Ending'] +\
					    		df_Starting['Opt_5_Ben Ending'] + df_Starting['Opt_6_Ben Ending'] +\
								df_Starting['Opt_7_Ben Ending']


print df_Starting['A_Pol_Fee Beg'].head(5)

df_Starting['OAP Start(Excl SD & PolFee)'] = np.where(df_Starting['OAP_start-temp'].isnull(),
													0, df_Starting['OAP_start-temp'] * (df_Starting['1%'] + df_Starting['2%'])/100)
df_Starting['OAP End(Excl SD & PolFee)'] = np.where(df_Starting['OAP_end-temp'].isnull(),
													0, df_Starting['OAP_end-temp'] * (df_Starting['1%'] + df_Starting['2%'])/100)
print ('Uncomment next 2 lines, when do-it fixed')
# df_Starting['Pol Fee Start'] = np.where(df_Starting['A_Pol_Fee Beg'].isnull(),
# 									0, df_Starting['A_Pol_Fee Beg'] * (df_Starting['1%'] + df_Starting['2%'])/100)
# df_Starting['Pol Fee End'] = np.where(df_Starting['A_Pol_Fee Ending'].isnull(),
# 									0, df_Starting['A_Pol_Fee Ending'] * (df_Starting['1%'] + df_Starting['2%'])/100)


# df_Starting['SD Start'] = np.where(df_Starting['A_Stamp_Duty Beg'].isnull(),
# 									0, df_Starting['A_Stamp_Duty Beg'] * (df_Starting['1%'] + df_Starting['2%'])/100)

df_Starting['SI End'] = np.where(df_Starting['SI Ending'].isnull(),
									0, df_Starting['SI Ending'] * (df_Starting['1%'] + df_Starting['2%'])/100)




df_Starting['SD End'] = np.where(df_Starting['A_Stamp_Duty Ending'].isnull(),
									0, df_Starting['A_Stamp_Duty Ending'] * (df_Starting['1%'] + df_Starting['2%'])/100)

print (df_Starting['OAP End(Excl SD & PolFee)'] + df_Starting['Pol Fee End'] + df_Starting['SD End']).head(10)
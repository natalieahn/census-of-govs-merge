# Code to merge US Census of Governments data, Finance and Employment surveys,
# local unit files, for years from 1967 to 2014
#
# By Natalie Ahn
#
# Originally written for use in several projects led by Sarah Anzia, on local government
# politics, interest groups, and public policy outcomes.
#
# Created: May 2017
# Updated: March 2018

import re, csv

# CHANGE THESE VARIABLES for specific years and U.S. places to merge
# (Note: command line options to specify these parameters from the shell coming soon)

start_year_fin = 2005
end_year_fin = 2015
start_year_emp = 2005
end_year_emp = 2016
place_types_to_include = ['Municipality'] # To specify only certain place types
# Place type options: 'State', 'County', 'Municipality', 'Township', 'Special District', 'Independent School District'

cog_dir = "Documents/cogmerge/" # Change to wherever you save this repo plus COG data files
fin_output_filename = cog_dir+'fin_data_merged.csv' # Change only if desired
emp_output_filename = cog_dir+'emp_data_merged.csv' # Change only if desired
sample_fips_codes_file = None # Change if you want to specify only certain places
# If specifying sample_fips_codes_file, code will disregard place_types_to_include (above).
# Use a plain text (.txt) file with one FIPS_State_Place code on each line.

# Change the below as needed based on the structure of the COG data files you've downloaded.
# To add a new year of data, hopefully it will be the same format as 2014/2015, just add a
# dictionary entry with tuple (year_folder, cities_filename, data_filename) like 2015 below.
fin_data_dir_pre13 = cog_dir+'_IndFin_1967-2012/'
fin_data_dir_post13 = cog_dir+'FinanceDataFiles_2013-2014/'
fin_data_files_post13 = {2013:(fin_data_dir_post13+'2013_Individual_Unit_File/','Fin_GID_2013.txt','2013FinEstDAT_09212015mod_pu.txt'),
						 2014:(fin_data_dir_post13+'2014_Individual_Unit_File/','Fin_GID_2014.txt','2014FinEstDAT_11232016mod_pu.txt'),
						 2015:(fin_data_dir_post13+'2015_Individual_Unit_File/','Fin_GID_2015.txt','2015FinEstDAT_08172017modp_pu.txt')}
emp_data_dir_pre11 = cog_dir+'_IndEmp1972-2010/'
emp_data_dir_post11 = cog_dir+'EmploymentDataFiles_2011-2015/'
emp_data_files_post11 = {2011:(emp_data_dir_post11+'2011 Individual Unit Files/','11empid.dat','11empst.dat'),
						 2012:(emp_data_dir_post11+'2012 Individual Unit Files/','12cempid.dat','12cempst.dat'),
						 2013:(emp_data_dir_post11+'2013 Individual Unit Files/','13empid.dat','13empst.dat'),
						 2014:(emp_data_dir_post11+'2014 Individual Unit Files/','14empid.txt','14empst.txt'),
						 2015:(emp_data_dir_post11+'2015 Individual Unit Files/','15empid.txt','15empst.txt'),
						 2016:(emp_data_dir_post11+'2016 Individual Unit Files/','16empid.txt','16empst.txt')}

# GOVS_ID to FIPS_State_Place codes:

print('Merging Census of Governments data for: %d-%d (Finance), %d-%d (Employment)' % (start_year_fin, end_year_fin, start_year_emp, end_year_emp))
print('Please wait, this may take a few minutes. Use CTRL-Z to terminate early.')

with open(cog_dir+'GOVS_ID_to_FIPS_Place_Codes_2012.csv', 'rU') as f:
	reader = csv.reader(f)
	rows = [row for row in reader]
	heads = rows[0]
	govs_i = heads.index('GOVS_ID')
	fips_st_i, fips_pl_i = heads.index('FIPS State Code'), heads.index('FIPS Place Code')
	govs_to_fips = {}
	for row in rows[1:]:
		govs_id = '%.9d' % int(row[govs_i])
		fips_state_place = '%.2d%.5d' % (int(row[fips_st_i]), int(row[fips_pl_i]))
		govs_to_fips[govs_id] = fips_state_place

fips_to_include = set()
if sample_fips_codes_file:
	with open(sample_fips_codes_file, 'r') as f:
		fips_to_include = set([line.strip() if len(line.strip())==7 \
							   else '0'+line.strip() for line in f])

# COG Finance:

place_type_codes = {'State':0,'County':1,'Municipality':2,'Township':3,
					'Special District':4,'Independent School District':5}
place_codes_to_include = [place_type_codes[pt] for pt in place_types_to_include]

# Merge 1967-2012 Files:

pre13_difs = {'-Oth Cap Outlay':['-Cap Outlay','-Construct'],
			  '-Curr Ops':['-Direct Exp','-Cap Outlay']}

def CalcDif(row_dict, var, var_suffix, part_suffixes):
	suffix_start = re.search(var_suffix, var).start()
	part_vars = []
	for part_suffix in part_suffixes:
		if var[:suffix_start] + part_suffix in row_dict:
			part_vars.append(var[:suffix_start] + part_suffix)
		elif part_suffix == '-Direct Exp' and var[:suffix_start] + '-Total Exp' in row_dict:
			part_vars.append(var[:suffix_start] + '-Total Exp')
	if len(part_vars) < 2: return ''
	return int(row_dict[part_vars[0]]) - \
		   sum([int(row_dict[part_var]) for part_var in part_vars[1:]])

with open(cog_dir+'fin_varname_conversion_table.csv', 'rU') as f:
	reader = csv.reader(f)
	rows = [row for row in reader]
	heads_cogvars = rows[0]
	var_std_i = heads_cogvars.index('variable_std')
	var_pre_i = heads_cogvars.index('variable_pre13')
	var_post_i = heads_cogvars.index('variable_post13')
	file_i = heads_cogvars.index('pre13_file')
	fin_vars_std = [row[var_std_i] for row in rows[1:]]
	fin_vars_pre_to_std = {row[var_pre_i]:row[var_std_i] for row in rows[1:]}
	fin_vars_post_to_std = {row[var_post_i]:row[var_std_i] for row in rows[1:]}
	fin_var_files = {row[var_std_i]:row[file_i] for row in rows[1:]}

def read_cog_file(filename):
	cog = {}
	with open(filename, 'rU') as f:
		reader = csv.reader(f)
		rows = [row for row in reader]
		heads = [x.strip() for x in rows[0]]
		id_i = heads.index('ID')
		for row in rows[1:]:
			if fips_to_include or int(row[id_i][2]) in place_codes_to_include:
				row_dict = {heads[i]:row[i] for i in range(len(heads))}
				row_dict_std = {fin_vars_pre_to_std[var]:value \
								for var,value in row_dict.items() if var in fin_vars_pre_to_std}
				if row_dict_std['GOVS_ID'] in govs_to_fips:
					row_dict_std['FIPS_State_Place'] = govs_to_fips[row_dict_std['GOVS_ID']]
				else: row_dict_std['FIPS_State_Place'] = ''
				if row_dict_std['GOVS Type Code'] == '4':
					row_dict_std['FunctCode'] = row_dict_std['Population']
					row_dict_std['Population'] = ''
				if not fips_to_include or row_dict_std['FIPS_State_Place'] in fips_to_include:
					cog[row[id_i]] = row_dict_std
	for std_var, var_file in fin_var_files.items():
		if var_file=='%s.calculate'%filename[-5:]:
			for sum_suffix, part_suffixes in pre13_difs.items():
				if re.search(sum_suffix, std_var):
					for place_id, row_dict in cog.items():
						row_dict[std_var] = CalcDif(row_dict, std_var, sum_suffix, part_suffixes)
	return cog

cog_files = {}
with open(fin_output_filename, 'w') as g:
	writer = csv.writer(g)
	x=writer.writerow(fin_vars_std)
	for year in range(start_year_fin, min(end_year_fin+1, 2013)):
		cog_files['a.Txt'] = read_cog_file(fin_data_dir_pre13+'IndFin%sa.Txt'%str(year)[-2:])
		cog_files['b.Txt'] = read_cog_file(fin_data_dir_pre13+'IndFin%sb.Txt'%str(year)[-2:])
		cog_files['c.Txt'] = read_cog_file(fin_data_dir_pre13+'IndFin%sc.Txt'%str(year)[-2:])
		for place_id, row_dict in cog_files['a.Txt'].items():
			merged_row = []
			for std_var in fin_vars_std:
				if fin_var_files[std_var] and place_id in cog_files[fin_var_files[std_var][:5]] \
				and std_var in cog_files[fin_var_files[std_var][:5]][place_id]:
					merged_row.append(cog_files[fin_var_files[std_var][:5]][place_id][std_var])
				else: merged_row.append('')
			x=writer.writerow(merged_row)


# Merge in 2013 and 2014 Files:

def ReadPost13DataFile(file_dir, city_file_name, data_file_name, year):
	with open(file_dir+city_file_name, 'r') as f:
		cog_data = {}
		for line in f:
			if fips_to_include or int(line[2]) in place_codes_to_include:
				row_dict = {'GOVS_ID':line[:9], 'Place Name':line[14:78], 'County Name':line[78:113],
							'FIPS State Code':line[113:115], 'FIPS County Code':line[115:118],
							'FIPS Place Code':line[118:123], 'Population':line[123:132],
							'YearPop':line[132:134],'FunctCode':line[143:145],'SchLevCode':line[145:147],
							'FYEndDate':line[147:151],'YearofData':line[151:153],
							'FIPS_State_Place':line[113:115]+line[118:123],'Year':year}
				if not fips_to_include or row_dict['FIPS_State_Place'] in fips_to_include:
					cog_data[row_dict['GOVS_ID']] = row_dict
	with open(file_dir+data_file_name, 'r') as f:
		vars_in_cog = set()
		for line in f:
			if int(line[2]) in place_codes_to_include:
				row_dict = {'GOVS_ID':line[:9], 'item_code':line[14:17], 'value':line[17:29]}
				if row_dict['GOVS_ID'] in cog_data:
					if row_dict['item_code'] in fin_vars_post_to_std:
						std_var = fin_vars_post_to_std[row_dict['item_code']]
						cog_data[row_dict['GOVS_ID']][std_var] = int(row_dict['value'])
						vars_in_cog.add(std_var)
	return cog_data, vars_in_cog

cog_post13_yrdicts = []
vars_in_cog = {}
for year in range(max(2013,start_year_fin), end_year_fin+1):
	if year in fin_data_files_post13:
		file_dir, city_file_name, data_file_name = fin_data_files_post13[year]
		cog_data, vars_in_cog_yr = ReadPost13DataFile(file_dir, city_file_name, data_file_name, year)
		cog_post13_yrdicts.append((year, cog_data))
		vars_in_cog[year] = vars_in_cog_yr

categ_sums = {'Total Educ':['Elem Educ','Higher Ed','Educ_NEC'],
			  'Total Highways':['Regular Hwy','Toll Hwy'],
			  'Public Welf':['Welf_Categ','Welf_Cash','Welf_Ins','Welf_NEC'],
			  'Total Util':['Water Util','Elec Util','Gas Util','Trans Util']}

for year,cog_dict in cog_post13_yrdicts:
	for city_id,city_vars in cog_dict.items():
		for var in fin_vars_std:
			match = re.match('(.+)\\-Total Exp', var)
			if match:
				part_vars = [part_var for part_var in city_vars \
							 if re.match(match.group(1), part_var) and part_var != var \
							 and not re.search('\\-Cap Outlay|\\-Direct Exp', part_var)]
				value = sum([city_vars[part_var] for part_var in part_vars])
				if value:
					city_vars[var] = value
					vars_in_cog[year].add(var)
		for var in fin_vars_std:
			match = re.match('(.+)\\-Cap Outlay', var)
			if match:
				part_vars = [match.group(1)+'-Construct', match.group(1)+'-Oth Cap Outlay']
				value = sum([city_vars[part_var] for part_var in part_vars \
									  if part_var in city_vars])
				if value:
					city_vars[var] = value
					vars_in_cog[year].add(var)
		for var in fin_vars_std:
			match = re.match('(.+)\\-Direct Exp', var)
			if match:
				part_vars = [match.group(1)+'-Curr Ops', match.group(1)+'-Cap Outlay']
				value = sum([city_vars[part_var] for part_var in part_vars \
									  if part_var in city_vars])
				if value:
					city_vars[var] = value
					vars_in_cog[year].add(var)
		for var in fin_vars_std:
			var_split = var.split('-')
			if var_split[0] in categ_sums:
				part_vars = [part_base+'-'+var_split[1] \
							 for part_base in categ_sums[var_split[0]]]
				value = sum([city_vars[part_var] for part_var in part_vars \
									  if part_var in city_vars])
				if value:
					city_vars[var] = value
					vars_in_cog[year].add(var)

with open(fin_output_filename, 'a') as g:
	writer = csv.writer(g)
	for year,cog_dict in cog_post13_yrdicts:
		for city_id,city_vars in cog_dict.items():
			std_row = []
			for var in fin_vars_std:
				if var in city_vars: std_row.append(city_vars[var])
				elif var in vars_in_cog[year]: std_row.append(0)
				else: std_row.append('')
			n=writer.writerow(std_row)


# COG Employment:

with open(cog_dir+'emp_varname_conversion_table.csv', 'rU') as f:
	reader = csv.reader(f)
	rows = [row for row in reader]
	heads_empvars = rows[0]
	var_std_i = heads_empvars.index('variable_std')
	var_pre_i = heads_empvars.index('variable_pre11')
	var_post_i = heads_empvars.index('variable_post11')
	emp_vars_std = [row[var_std_i] for row in rows[1:]]
	emp_vars_pre_to_std = {row[var_pre_i]:row[var_std_i] for row in rows[1:]}
	emp_vars_post_to_std = {int(row[var_post_i]): '-'.join(row[var_std_i].split('-')[:-1]) \
							for row in rows[1:] if row[var_post_i]}

# For years before 2011:

emp_prefixes = set(['-'.join(var.split('-')[:-1]) for var in emp_vars_std if '-' in var])
pre11_difs = {'-PT Emp':['-Total Emp','-FT Emp'],
			  '-PT Pay':['-Total Pay','-FT Pay']}

def ReadEmpFilePre11(filename):
	with open(filename, 'rU') as f:
		reader = csv.reader(f)
		rows = [row for row in reader]
		heads = [x.strip() for x in rows[0]]
		id_i = heads.index('ID')
		emp_data = {}
		for row in rows[1:]:
			row_dict = {heads[i]:row[i] for i in range(len(heads))}
			std_row_dict = {emp_vars_pre_to_std[var]:value for var,value in row_dict.items()\
							if var in emp_vars_pre_to_std}
			if std_row_dict['GOVS_ID'] in govs_to_fips:
				std_row_dict['FIPS_State_Place'] = govs_to_fips[std_row_dict['GOVS_ID']]
			else: std_row_dict['FIPS_State_Place'] = ''
			if (not fips_to_include and int(std_row_dict['GOVS Type Code']) in place_codes_to_include) \
			or (fips_to_include and std_row_dict['FIPS_State_Place'] in fips_to_include):
				for prefix in emp_prefixes:
					for var_suffix, part_suffixes in pre11_difs.items():
						std_row_dict[prefix+var_suffix] = CalcDif(std_row_dict,
											prefix+var_suffix, var_suffix, part_suffixes)
				emp_data[row[id_i]] = std_row_dict
	return emp_data

with open(emp_output_filename, 'w') as g:
	writer = csv.writer(g)
	n=writer.writerow(emp_vars_std)
	for year in range(start_year_emp, min(end_year_emp+1, 2011)):
		emp_data = ReadEmpFilePre11(emp_data_dir_pre11+'IndEmp%s.Txt'%str(year)[-2:])
		for city_id, city_vars in emp_data.items():
			std_row = [city_vars[std_var] if std_var in city_vars else '' \
					   for std_var in emp_vars_std]
			n=writer.writerow(std_row)

# For years after 2011:

emp_suffixes = ['FT Emp','FT Pay','PT Emp','PT Pay','FT Equiv']

categ_sums = [('Elem Educ',['Elem Educ-Instr','Elem Educ-Other']),
			  ('Total High Ed',['High Ed-Instr','High Ed-Other']),
			  ('Total Educ',['Elem Educ','Total High Ed']),
			  ('Fire Prot',['Firefighters','Fire-Other']),
			  ('Police Prot',['Police Officers','Police-Other']),
			  ('Health & Hosp',['Health','Hospital']),
			  ('Sanitation',['Sewerage','SW Mgmt']),
			  ('Total Util',['Water Util','Elec Util','Gas Util','Transit Util'])]

emp_sums = {'Total Emp':['FT Emp','PT Emp'],
			'Total Pay':['FT Pay','PT Pay']}

def CalcSum(row_dict, part_vars):
	part_vals = [int(row_dict[part_var]) for part_var in part_vars if part_var in row_dict]
	if not part_vals: return 0
	return sum(part_vals)

def MergeEmpDataPost11(file_dir, city_file_name, data_file_name, out_file_writer, year):
	with open(file_dir+city_file_name, 'r') as f:
		cog_data, vars_in_cog = {}, set()
		for line in f:
			if fips_to_include or int(line[2]) in place_codes_to_include:
				row_dict = {'Year':year, 'GOVS_ID':line[:9], 'GOVS Type Code':line[2],
							'Place Name':line[14:78], 'County Name':line[79:109], 'Census Region':line[78],
							'FIPS State Code':line[109:111], 'FIPS County Code':line[111:114],
							'Population':line[125:134], 'YearPop':line[134:136],
							'SchLevCode':line[136:138],'FTPayCode':line[189],'PTPayCode':line[190],
							'Months_Teachers_Paid':line[191:193],'Months_Sch_Admin_Paid':line[193:195],
							'Months_Sch_Other_Paid':line[195:197],
							'YearData':line[197:199],'YearDepSch':line[199:201]}
				if row_dict['GOVS_ID'] in govs_to_fips:
					row_dict['FIPS_State_Place'] = govs_to_fips[row_dict['GOVS_ID']]
				else: row_dict['FIPS_State_Place'] = ''
				if not fips_to_include or row_dict['FIPS_State_Place'] in fips_to_include:
					cog_data[row_dict['GOVS_ID']] = row_dict
	with open(file_dir+data_file_name, 'r') as f:
		for line in f:
			if int(line[2]) in place_codes_to_include:
				row_dict = {'GOVS_ID':line[:9], 'item_code':int(line[17:20]),
							'FT Emp':line[20:30], 'FT Pay':line[32:44],
							'PT Emp':line[46:56], 'PT Pay':line[58:70],
							'PT Hours':line[72:82], 'FT Equiv':line[84:94]}
				if row_dict['GOVS_ID'] in cog_data:
					if row_dict['item_code'] in emp_vars_post_to_std:
						std_var = emp_vars_post_to_std[row_dict['item_code']]
						for suffix in emp_suffixes:
							cog_data[row_dict['GOVS_ID']][std_var+'-'+suffix] = row_dict[suffix]
							if cog_data[row_dict['GOVS_ID']][std_var+'-'+suffix]:
								vars_in_cog.add(std_var+'-'+suffix)
	for row_dict in cog_data.values():
		for sum_prefix, part_prefixes in categ_sums:
			for suffix in emp_suffixes:
				part_vars = [part_prefix+'-'+suffix for part_prefix in part_prefixes]
				row_dict[sum_prefix+'-'+suffix] = CalcSum(row_dict, part_vars)
				if row_dict[sum_prefix+'-'+suffix]:
					vars_in_cog.add(sum_prefix+'-'+suffix)
		for sum_suffix, part_suffixes in emp_sums.items():
			for prefix in emp_prefixes:
				part_vars = [prefix+'-'+part_suffix for part_suffix in part_suffixes]
				value = CalcSum(row_dict, part_vars)
				if value:
					row_dict[prefix+'-'+sum_suffix] = value
					vars_in_cog.add(prefix+'-'+sum_suffix)
	for row_dict in cog_data.values():
		std_row = []
		for std_var in emp_vars_std:
			if std_var in row_dict: std_row.append(row_dict[std_var])
			elif std_var in vars_in_cog: std_row.append(0)
			else: std_row.append('')
#		std_row = [row_dict[std_var] if std_var in row_dict else '' \
#				   for std_var in emp_vars_std]
		n=out_file_writer.writerow(std_row)

with open(emp_output_filename, 'a') as g:
	writer = csv.writer(g)
	for year in range(max(2011, start_year_emp), end_year_emp+1):
		if year in emp_data_files_post11:
			file_dir, city_file_name, data_file_name = emp_data_files_post11[year]
			MergeEmpDataPost11(file_dir, city_file_name, data_file_name, writer, year)






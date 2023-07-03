import os
import datetime
import math

# https://study.healthsense.org/?partition=3
# a2ff38833dd20a1a3a1c79cb94555ff2
# arn:aws:secretsmanager:us-east-2:155524030570:secret:hs-relational-storage-partition-3-admin-password-vrIOuP

#use Studies1; 

#SELECT DISTINCT * FROM ParticipantDeviceLog where ParticipantId = 40 and ParticipantDeviceId=44 and WhenUtc>='2020-08-26 18:00:01.000' and WhenUtc<'2020-08-26 23:06:59.000'; and Event <> 'Usage'; 
#use Studies1; SELECT DISTINCT * FROM ParticipantDeviceLog where ParticipantDeviceId=77 and WhenUtc>='2020-10-28 05:00:01.000' and WhenUtc<'2020-10-29 05:59:59.000';



def populate_ts(strt, stop, usr, app):
	f = False
	for mini, t in enumerate(time_stamps):
		if t.time()>=strt:
			f =  True
			break
	for maxi, t in enumerate(time_stamps):
		if t.time()>=stop:
			break
	min_idx = mini
	max_idx = maxi
	#print min_idx, max_idx, strt, stop, app, usr
	
	for k in range(min_idx,max_idx+1):
		user_ts[k] = usr
		app_ts[k] = app
		usage_ts[k] = "PhoneUse"
		
	return 

def add_app_time(app_stats_dict, app_name, use_secs, start_time, stop_time):
	#print app_stats_dict
	if app_name in app_stats_dict.keys():
		app_stats_dict[app_name][0] = app_stats_dict[app_name][0] + use_secs
		app_stats_dict[app_name][1].append([start_time, stop_time])
	else:
		app_stats_dict[app_name] = [use_secs,[[start_time, stop_time]]]

	return app_stats_dict
	
def process(lines, start_time,type_events):
	date_ = str(start_time.date())
	new_lines = []
	ts_ls = []
	for line in lines:
		line_ = line.strip('\r\n')
		line_ = line_.split(',')
		#print line
		#print line[1:11], line_[-1], type_events[:2], date_
		if line[:10]==date_ and line_[-2] in type_events[:2]:
			new_lines.append(line)
			ts_ls.append(line_[0])
	
	temp = [(v,i) for i,v in enumerate(ts_ls)]
	temp.sort()
	ts_sorted, indices  = zip(*temp)
	sorted_lines = []
	for idx in indices:
		sorted_lines.append(new_lines[idx])
	
	fid = open('tmp.txt','w')
	for line in sorted_lines:
		fid.write(line)
	fid.close()
	#print sorted_lines
	return sorted_lines

def processv2(lines, start_time,type_events):
	#date_ = str(start_time.date())
	new_lines = []
	ts_ls = []
	for line in lines:
		line_ = line.strip('\r\n')
		line_ = line_.split(',')
		#print line_
		#print line[1:11], line_[-1], type_events[:2], date_
		if line_[-7] in type_events[:2]:
			new_lines.append(line)
			#print(line_)
			ts_ls.append(line_[5])

	temp = [(v,i) for i,v in enumerate(ts_ls)]
	temp.sort()
	ts_sorted, indices  = zip(*temp)
	sorted_lines = []
	for idx in indices:
		sorted_lines.append(new_lines[idx])

	
	fid = open('tmp.csv','w')
	for line in sorted_lines:
		fid.write(line)
	fid.close()
	
	#print sorted_lines
	return sorted_lines
	
type_events = ['Move to Foreground', 'Move to Background', 'Configuration Change', 'User Interaction']
#date_time_str = '2018-06-29 08:15:27.243860'
#date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')


# Edgar_data_19thNov
# Saira_data_19thNov
# Edgar_data_14thNov

#fname = './beta_test_4/famXXX_pid61/May03_May04.csv'
#fname = '/home/akv/Flash/Chronicle_test/Ori_rawdata_103_proc.csv'
#fname = 'Chronicle_test/Tat_Oct26.csv' #ChronicleData_20220214-555
#fname = '/home/akv/Flash/bt_validation_study/study3/study3_chronicle/576_chronicle data.csv' #ChronicleData_20220214-555
#fname = '/home/akv/FLASH_PO1/data/582/582_chron.csv' #ChronicleData_20220214-555
fname = '/home/akv/Downloads/gnsm_mrn_t1.csv'

fid = open('tmp_check.csv','w')
#fname = 'validation_study/ChronicleData_20220113-514.csv'
condensed = False
print fname


f = open(fname, 'r')
lines = f.readlines()
print(len(lines))
#lines = lines[::-1]
#lines = lines[:-1]
lines = lines[1:]

#576, 11:44:24am, 5, 03/15/2023
start_time = datetime.datetime(2023, 6, 24, 0, 0, 0,1)
end_time = datetime.datetime(2023, 6, 24, 23, 59, 4,0)
delta = datetime.timedelta(microseconds=166670)

time_stamps = []
t = start_time
while t<=end_time:
	time_stamps.append(t)
	t = t+delta

user_ts = ["None" for i in range(len(time_stamps))]
app_ts = ["None" for i in range(len(time_stamps))]
usage_ts = ["No-PhoneUse" for i in range(len(time_stamps))]

lines = processv2(lines, start_time, type_events)

#print lines[:100]
print 'Num Lines: ', len(lines)


used_apps = {}
use_time = {'Child':0, 'Other':0}

used_apps_tmp = {}
user = None
user_st = None


print ''.join(['*']*65)
print '| %13s | %13s | %8s | %8s | %7s | %10s' % ('User ID', 'APP Name', 'start_T', 'stop_T', 'use', 'app details')
print ''.join(['*']*65)


rows = []
diff = False
need_print = False

unlock_login = 0
child_login = 0
usr_known_time = 0.0
usr_known_app_time = 0.0
app_time = 0.0
usr_unknown_time = 0.0
usr_unknown_app_time = 0.0
unlock_nologin = 0
unlock_time = start_time
prev_unlock_time = start_time
screen_off_time = start_time
login_time = start_time

unlock = False
login = False

app_stats = {}

for line in lines[0:]:
	line = line.strip('\r\n')
	line = line.split(',')
	#line = line[1:]
	app_name = line[2]
	
	#print app_name, event_type
	time_str = line[5]
	try:
		time_obj = datetime.datetime.strptime(time_str[:-6], '%Y-%m-%dT%H:%M:%S.%f')
	except ValueError:
		time_obj = datetime.datetime.strptime(time_str[:-6], '%Y-%m-%dT%H:%M:%S')
		
	time_obj = time_obj#-datetime.timedelta(hours=12)#-datetime.timedelta(hours=12) # conversion from UTC time to Central time, US
	
	user=line[-3]
	if user == 'Target child':
		user='Child'
	if user == '':
		user=None

	# time_obj.date()==datetime.date(2019, 12, 23)
	if time_obj>=start_time and time_obj<=end_time: 
		usage_type = type_events.index(line[-7])+1
		#print 1	
		#print line				
		if app_name in used_apps_tmp.keys():
			#print 'first', app_name, len(rows)
			if used_apps_tmp[app_name][1] is None and usage_type==2:
				#print 'is end event'
				used_apps_tmp[app_name][1] = time_obj # stop time append
				
				app_use_time = time_obj - used_apps_tmp[app_name][0]
				app_use_secs = app_use_time.total_seconds()
				
				if app_use_secs > 1.0:
					#print 'is usage >1'
					#print user, app_name.split('.')[-1], 'start time', used_apps_tmp[app_name][0], app_use_secs
					app_start_time = used_apps_tmp[app_name][0]
					app_start_user = used_apps_tmp[app_name][2]
					if user is not None:
						user_st = user
					elif app_start_user is not None:
						user_st = app_start_user
					else:
						user_st = None
						
					start_t = app_start_time.time()
					#print start_time
					app_start_time = app_start_time.replace(microsecond=0)#.time()
					app_stop_time = used_apps_tmp[app_name][1]
					stop_t = app_stop_time.time()
					#print stop_time
					app_stop_time = app_stop_time.replace(microsecond=0)#.time()
					use = app_use_secs/60.0
					use_m = math.floor(use)
					use_s = int((use - use_m) * 60.0)
					if user_st is not None:
						use_time[user_st] = use_time[user_st] + app_use_secs
					row = [user_st, app_name.split('.')[-1][:13], app_start_time, app_stop_time, use_m, use_s, app_name] # '.'.join(app_name.split('.')[-2:])
					
					
					if len(rows)>=1 and condensed:
						curr_row = row
						prev_row = rows[-1]
						diff_time = curr_row[2] - prev_row[3]
						diff_secs = diff_time.total_seconds()
						#print app_name, len(rows)
						if curr_row[0]==prev_row[0] and curr_row[1]==prev_row[1] and diff_secs<10: # are the users same and app same
							user_track = user_st
							app_track = app_name.split('.')[-1][:13]
							app_name_track = app_name
							app_start_track = prev_row[2]
							row[2] = app_start_track
							app_stop_track = curr_row[3]
							need_print = True
							diff = False
							#print 'same identified'
							#print 'SS| %13s | %13s | %8s | %8s | %2dm %2ds | %20s ' % (user, app_name.split('.')[-1][:13], app_start_time, app_stop_time, use_m, use_s, '.'.join(app_name.split('.')[-2:]))
						else:
							#print 'diff print needed'
							#print 'apps:', prev_row[1], curr_row[1]
							#print prev and current
							if need_print:
								need_print=False
								#print 'print needed'
								lapp_use_time = app_stop_track - app_start_track
								lapp_use_secs = lapp_use_time.total_seconds()
								use = lapp_use_secs/60.0
								use_m = math.floor(use)
								use_s = int((use - use_m) * 60.0)
								if lapp_use_secs > 30: #and user_track=='Other':  
									print '| %13s | %13s | %8s | %8s | %2dm%2ds | %20s ' % (user_track, app_track, app_start_track.time(), app_stop_track.time(), use_m, use_s, '.'.join(app_name_track.split('.')[-2:]))
									write_line = [user_track, app_track, app_start_track, app_stop_track, '.'.join(app_name_track.split('.')[-2:])]
									write_line = [str(line_item) for line_item in write_line]
									fid.write(','.join(write_line)+'\n')
									app_stats = add_app_time(app_stats, '.'.join(app_name_track.split('.')[-2:]), lapp_use_secs, app_start_track, app_stop_track)
							if diff:
								#print 'diff print ted'
								# print prev row
								tmp_app_use_time = prev_row[3] - prev_row[2]
								if tmp_app_use_time.total_seconds() > 30: # and prev_row[0]=='Other':
									print '| %13s | %13s | %8s | %8s | %2dm%2ds | %20s ' % (prev_row[0], prev_row[1], prev_row[2].time(), prev_row[3].time(), prev_row[4], prev_row[5], '.'.join(prev_row[-1].split('.')[-2:]))
									app_stats = add_app_time(app_stats, '.'.join(prev_row[-1].split('.')[-2:]), tmp_app_use_time.total_seconds(), prev_row[2], prev_row[3])
									write_line = [prev_row[0],  prev_row[1], prev_row[2], prev_row[3], '.'.join(prev_row[-1].split('.')[-2:])]
									write_line = [str(line_item) for line_item in write_line]
									fid.write(','.join(write_line)+'\n')
							diff=True
							#print '| %13s | %13s | %8s | %8s | %2dm %2ds | %20s ' % (user, app_name.split('.')[-1][:13], app_start_time.time(), app_stop_time.time(), use_m, use_s, '.'.join(app_name.split('.')[-2:]))
							
					if len(rows)==0:
						#print '| %13s | %13s | %8s | %8s | %2dm%2ds | %20s ' % (user, app_name.split('.')[-1][:13], app_start_time.time(), app_stop_time.time(), use_m, use_s, '.'.join(app_name.split('.')[-2:]))
						tmp_app_use_time = app_stop_time - app_start_time
						app_stats = add_app_time(app_stats, '.'.join(app_name.split('.')[-2:]), tmp_app_use_time.total_seconds(), app_start_time, app_stop_time)
					rows.append(row)
					
					populate_ts(start_t, stop_t, user_st, '.'.join(app_name.split('.')[-2:]))
					if not condensed:
						tmp_app_use_time = app_stop_time - app_start_time
						if tmp_app_use_time.total_seconds()>=5.0: # and user_st is None: # and user_st == 'Child':
							print '| %13s | %13s | %8s | %8s | %2dm%2ds | %20s ' % (user_st, app_name.split('.')[-1][:13], app_start_time.time(), app_stop_time.time(), use_m, use_s, '.'.join(app_name.split('.')[-2:]))
							tmp=10
						
						app_time = app_time + tmp_app_use_time.total_seconds()
						app_stats = add_app_time(app_stats, '.'.join(app_name.split('.')[-2:]), tmp_app_use_time.total_seconds(), app_start_time, app_stop_time)
						
						if user_st is not None:
							usr_known_app_time = usr_known_app_time + tmp_app_use_time.total_seconds()
						else:
							usr_unknown_app_time = usr_unknown_app_time + tmp_app_use_time.total_seconds()
					
					use = app_use_secs/60.0
					use_m = math.floor(use)
					use_s = int((use - use_m) * 60.0)
					#print 'LLLL| %13s | %13s | %8s | %8s | %2dm %2ds | %20s ' % (user, app_name.split('.')[-1][:13], app_start_time.time(), app_stop_time.time(), use_m, use_s, '.'.join(app_name.split('.')[-2:]))
					# if start stop diff > T, push event into the usedapps 
					
				# delete it from the tmp 
				del used_apps_tmp[app_name]
				
			elif usage_type==1:
				# start none
				#print 'Replacing the start time'				
				# repeated start
				used_apps_tmp[app_name][0] = time_obj # replace start time
				used_apps_tmp[app_name][2] = user
				tmp = 10
		else: # first time app opened
			if usage_type==1:
				used_apps_tmp[app_name] = [time_obj, None, user] # start time append
				#print app_name, ' start event', time_obj
			else:
				tmp = 10
				#print app_name, ' starts with end event'	

if need_print:
	need_print=False
	lapp_use_time = app_stop_track - app_start_track
	lapp_use_secs = lapp_use_time.total_seconds()
	use = lapp_use_secs/60.0
	use_m = math.floor(use)
	use_s = int((use - use_m) * 60.0)
	if lapp_use_secs > 7: #and user_track=='Other':  
		print '| %13s | %13s | %8s | %8s | %2dm%2ds | %20s ' % (user_track, app_track, app_start_track.time(), app_stop_track.time(), use_m, use_s, '.'.join(app_name_track.split('.')[-2:]))
		write_line = [user_track, app_track, app_start_track, app_stop_track, '.'.join(app_name_track.split('.')[-2:])]
		write_line = [str(line_item) for line_item in write_line]
		fid.write(','.join(write_line)+'\n')
		app_stats = add_app_time(app_stats, '.'.join(app_name_track.split('.')[-2:]), lapp_use_secs, app_start_track, app_stop_track)

print ''.join(['*']*65)

print used_apps_tmp
print use_time

print 'unlock login', unlock_login, 'unlock nologin', unlock_nologin, 'child login', child_login

print 'user known time', usr_known_time/60.0
print 'user unknown time', usr_unknown_time/60.0

print 'user known app time', usr_known_app_time/60.0
print 'user unknown app time', usr_unknown_app_time/60.0
print 'total', (usr_known_app_time + usr_unknown_app_time)/60.0

fid.close()




fid = open('tmp.txt','w')

#for i in range(1000):
c = 1
for i in range(len(time_stamps)):
	usage = "No-Scr-Use"
	if user_ts[i] == 'Child':
		usage = 'TC-Scr-Use'
	elif user_ts[i] == 'Other':
		usage = 'Other-Scr-Use'
		
	
	#s = ' '.join([str(time_stamps[i]), user_ts[i], usage_ts[i], app_ts[i]])
	s = ' '.join([str(c), str(time_stamps[i]), usage, app_ts[i]])
	fid.write(s+'\n')
	c+=1

fid.close()



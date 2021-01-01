import firebase_admin
from firebase_admin import credentials
from firebase_admin import auth
from firebase_admin import firestore
from firebase_admin import storage
import datetime
from datetime import timezone
import csv
import copy

def compare(cur, prev):
	if cur is None or prev is None:
		return None
	return round(cur - prev, 2)

def isAllNone(listt):
	for i in listt[2:]:
		if i is not None:
			return False
	return True

def removeBlanks(listt):
	for i in range(len(listt)):
		if listt[i] == None or listt[i] == "":
			listt[i] = "-"
	return listt


cred = credentials.Certificate('./serviceAccountKey.json')
firebase_admin.initialize_app(cred, { 'storageBucket': 'monthly-change-reports' })
db = firestore.client()
bucket = storage.bucket()
DESCENDING = firestore.Query.DESCENDING
ASCENDING = firestore.Query.ASCENDING

UIDS = [('0tPGnN8kdwP8TfMPInmulOfxJnF2', -8), # progread@soboba.net
		('HsO6si6GalMnSxYhl5Y5yBHIV5n2', -8), # wrphoto@wendoverfun.com
		('LMaZ7oaPxXXVAiKnneBg8Eqkw912', -8), # wpphoto@wendoverfun.com
		('M8ABeaNkhDWKl7yloZnRDFVEWoy1', -8), # wmphoto@wendoverfun.com
		('R1YI9Iaju6eOrVATToBEqAv5SdB3', -8), # rcphoto@peppermillreno.com
		('kw6XxJSElZM6BMHBir8LpUd3k1O2', -8), # meters@muckleshootcasino.com
		('oEa70GfrUoTq6Vi3rP2AywjNWyj1', -8), # progressiveread@swinomishcasino.com
		('yTQUJPc0uuP7foHJ2ZQ92KU3UM92', -8), # wvphoto@peppermillcas.com
		('rB2iRF4fEWadcsXhtiOO3gWXwvw2', -6), # progread@ccrla.com

		### For testing ###
		('1kyN8HCbC6gfZY8nNIYB1HjqRnH3', -8), # joe@capturemeters.com
		('xgdRnVu3yrgjEhrMQgDSImBEOCc2', -5)] # lotrrox@gmail.com

'''startDate = datetime.datetime(2020, 10, 30, 17, 00, tzinfo=timezone.utc)
endDate = datetime.datetime(2020, 12, 1, 0, 0, tzinfo=timezone.utc)
lastDayOfMonth = datetime.datetime(2020, 11, 30, 17, 00, tzinfo=timezone.utc)'''

monthName = 'december'

startDate = datetime.datetime(2020, 11, 29, 17, 00, tzinfo=timezone.utc)
endDate = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
lastDayOfMonth = datetime.datetime(2020, 12, 31, 17, 00, tzinfo=timezone.utc)
machineIds = set()

counter = 0

for item in UIDS:
	uid = item[0]
	offset = item[1]
	resetDate = startDate - datetime.timedelta(hours=offset)
	scansRef = db.collection(u'users').document(uid).collection(u'scans')
	query = scansRef.where(u'timestamp', u'>=', resetDate).where(u'timestamp', u'<=', endDate).order_by(u'timestamp', direction=ASCENDING)
	docs = query.stream()
	
	allDocs = []
	for doc in docs:
		counter += 1
		id = doc.to_dict()["machine_id"]
		#print(id)
		machineIds.add(id)
		allDocs.append(doc)
	#resetDate += datetime.timedelta(days=1)
	startRange = resetDate
	endRange = startRange + datetime.timedelta(days=1)
	allDays = []
	runningDays = []
	while endRange <= lastDayOfMonth - datetime.timedelta(hours=offset):
		count = 0
		for doc in allDocs:
			count += 1
			data = doc.to_dict()
			timestamp = data['timestamp']
			time = datetime.datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second, 0, timestamp.tzinfo)
			if time > startRange and time <= endRange:
				runningDays.append(data)
			#else:
				#break
		#print(allDocs)
		allDays.append(runningDays)
		runningDays = []
		startRange += datetime.timedelta(days=1)
		endRange += datetime.timedelta(days=1)


	emptyDict = { 1: [], 2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: [], 9: [], 10: [] }
	titles = ['progressive1', 'progressive2', 'progressive3', 'progressive4', 'progressive5', 'progressive6', 'progressive7', 'progressive8', 'progressive9', 'progressive10']
	output = {}
	for id in machineIds:
		output[id] = copy.deepcopy(emptyDict)

	for day in allDays:
		machines = copy.copy(machineIds)
		tmpScans = set()
		for scan in day:
			
			# Checks to ensure a machine is not scanned more than once per day.  If so, the earliest scan is used
			if scan['machine_id'] in tmpScans:
				break # skip and move onto next day
			else:
				tmpScans.add(scan['machine_id'])

			for i in range(10):
				try:
					output[scan['machine_id']][i + 1].append(float(scan[titles[i]]))
				except ValueError:
					output[scan['machine_id']][i + 1].append(None)
			if scan['machine_id'] in machines:
				machines.remove(scan['machine_id'])
		for id in machines:
			for i in range(10):
				output[id][i + 1].append(None)

	fileName = 'generated/' + monthName + '/' + uid + '.csv'
	with open(fileName, mode='w') as report_file:
		writer = csv.writer(report_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		#writer.writerow(['machine_id', 'progressive index', '10/31', '11/1', 'Change', '11/2', 'Change', '11/3', 'Change', '11/4', 'Change', '11/5', 'Change', '11/6', 'Change', '11/7', 'Change', '11/8', 'Change', '11/9', 'Change', '11/10', 'Change', '11/11', 'Change', '11/12', 'Change', '11/13', 'Change', '11/14', 'Change', '11/15', 'Change', '11/16', 'Change', '11/17', 'Change', '11/18', 'Change', '11/19', 'Change', '11/20', 'Change', '11/21', 'Change', '11/22', 'Change', '11/23', 'Change', '11/24', 'Change', '11/25', 'Change', '11/26', 'Change', '11/27', 'Change', '11/28', 'Change', '11/29', 'Change', '11/30', 'Change'])
		writer.writerow(['machine_id', 'progressive index', '11/30', '12/1', 'Change', '12/2', 'Change', '12/3', 'Change', '12/4', 'Change', '12/5', 'Change', '12/6', 'Change', '12/7', 'Change', '12/8', 'Change', '12/9', 'Change', '12/10', 'Change', '12/11', 'Change', '12/12', 'Change', '12/13', 'Change', '12/14', 'Change', '12/15', 'Change', '12/16', 'Change', '12/17', 'Change', '12/18', 'Change', '12/19', 'Change', '12/20', 'Change', '12/21', 'Change', '12/22', 'Change', '12/23', 'Change', '12/24', 'Change', '12/25', 'Change', '12/26', 'Change', '12/27', 'Change', '12/28', 'Change', '12/29', 'Change', '12/30', 'Change', '12/31', 'Change'])
		for i in output:
			for j in range(10):
				row = []
				row.append(i)
				row.append('p' + str(j + 1))
				#row.extend(output[i][j + 1])
				prev = None
				performCalculation = False
				for k in output[i][j + 1]:
					row.append(k)
					if performCalculation: # calculate change
						row.append(compare(k, prev))
					prev = k
					performCalculation = True
				if not isAllNone(row):
					writer.writerow(removeBlanks(row))

	blob = bucket.blob(uid + '/december.csv')
	blob.upload_from_filename('/Users/alexanderpowell/Desktop/projects/mic-monthly-report-generator/' + fileName)
	print('Report created for uid: ' + uid)

print(str(counter) + ' rows fetched in total')








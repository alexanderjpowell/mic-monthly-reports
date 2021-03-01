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

def formatRow(listt):
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
		('6KVU4l5JA6Oqz3v78czw9cUBdwx1', -8), # casinoaccounting@cosmopolitanlasvegas.com
		('KJ34GdRb7PPj4QLm0zDWZ6Sk9fC3', -8), # acrmmeters@accmail.net
		('Oko1D0siR7Q7NneCOCe0tLdRw7J3', -8), # acpsmeters@srcmail.net
		('ZXOj33tfp6SrZ7QB0txYL1toAIF3', -8), # acccmeters@accmail.net
		('uJjfFFwtnyL8YKFMBATkJW57BNZ2', -8), # mic@gratonresortcasino.com

		### For testing ###
		('1kyN8HCbC6gfZY8nNIYB1HjqRnH3', -8), # joe@capturemeters.com
		('xgdRnVu3yrgjEhrMQgDSImBEOCc2', -5)] # lotrrox@gmail.com

monthName = 'february'

'''startDate = datetime.datetime(2020, 11, 29, 17, 00, tzinfo=timezone.utc)
endDate = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
lastDayOfMonth = datetime.datetime(2020, 12, 31, 17, 00, tzinfo=timezone.utc)'''
# startDate = datetime.datetime(2020, 12, 31, 17, 00, tzinfo=timezone.utc)
# endDate = datetime.datetime(2021, 2, 1, 0, 0, tzinfo=timezone.utc)
# lastDayOfMonth = datetime.datetime(2021, 1, 31, 17, 00, tzinfo=timezone.utc)

# Reset time is 5pm local
startDate = datetime.datetime(2021, 1, 30, 17, 0, tzinfo=timezone.utc)
endDate = datetime.datetime(2021, 3, 1, 17, 0, tzinfo=timezone.utc)

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
	while endRange <= endDate - datetime.timedelta(hours=offset):
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
		header = ['machine_id', 'progressive index', '1/31', '2/1', 'Change', '2/2', 'Change', '2/3', 'Change', '2/4', 'Change', '2/5', 'Change', '2/6', 'Change', '2/7', 'Change', '2/8', 'Change', '2/9', 'Change', '2/10', 'Change', '2/11', 'Change', '2/12', 'Change', '2/13', 'Change', '2/14', 'Change', '2/15', 'Change', '2/16', 'Change', '2/17', 'Change', '2/18', 'Change', '2/19', 'Change', '2/20', 'Change', '2/21', 'Change', '2/22', 'Change', '2/23', 'Change', '2/24', 'Change', '2/25', 'Change', '2/26', 'Change', '2/27', 'Change', '2/28', 'Change']
		writer.writerow(header)
		headerLength = len(header)
		for i in output:
			for j in range(10):
				row = []
				row.append(i)
				row.append('p' + str(j + 1))
				prev = None
				performCalculation = False
				for k in output[i][j + 1]:
					row.append(k)
					if performCalculation: # calculate change
						row.append(compare(k, prev))
					prev = k
					performCalculation = True
				if not isAllNone(row):
					writer.writerow(formatRow(row)[:headerLength])

	blob = bucket.blob(uid + '/february2021.csv')
	blob.upload_from_filename('/Users/alexanderpowell/Desktop/projects/mic-monthly-report-generator/' + fileName)
	print('Report created for uid: ' + uid)

print(str(counter) + ' rows fetched in total')








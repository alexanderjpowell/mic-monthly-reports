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
		('0uBA0S1nqXfMs2YYnoYdREVoM2Q2', -8), # sevans@peppermillreno.com
		('69yPfdP277dZDAMP4TIpyb3ZSwx2', -8), # rhsvc@rollinghillscasino.com
		('Q86qgiOu6UcrRF97OHmYeIyCT6a2', -8), # tachipalace@mic.com
		('rLhBmUAqIBXUNthKbWrlSaiyn832', -8), # mtrread@spc.com
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
		('WZ2c9X1QCzRkgE0HYGKgAHIVYQC3', -8), # rhsvcmic@rollinghillscasino.com
		('2gR3TX7zR9P6KwTjffPmizw0MF23', -8), # chickenranch@mic.com
		('Cc8hmrfnMaRizETqyCqSgxll9n62', -8), # stlvprogressivemeter@boydgaming.com
		('sznI4hVPWcP6h3dw4LuLYgNzpJK2', -8), # azcwmeters@goldenent.com
		('W8c5VOXRWHMdqMkwLfVxWxkxz393', -8), # vvmeters@mic.com
		('e0Y8noCWQNX8w4iBzRi5WitePAz1', -6), # prairiebandmeters@mic.com

		('BBPHTXVe53XZPv0HD3mLnUBdEMF3', -8), # g2e@everi.com
		('lOayiEsclAchyOUSrGotmoldKl42', -8), # eaglemountainmeters@mic.com
		('SN0GLxWln7Uiwc4ay8YeJ4Eu1zg2', -8), # jeffrey.hoss@everi.com
		('D4vMsOCN9rP1V2n3O91gikqpPY22', -5), # fwd@mic.com
		('sPFTqKf0ygdPpWGPyGeYEbwM91t1', -5), # fwh@mic.com
		('Isadzx74ThSU4FQwoO2ftivoZq23', -5), # fwnb@mic.com
		('pOKeHHjgRRPeWVUzWMvsrzcBddk2', -5), # fwsb@mic.com
		('eUobdthlVRcW84qLgBdVOPsSZwl2', -5), # fourwindsmeters@mic.com
		('bKy7OVZernbqUZB2Ht14vRGQR5g1', -5), # earthcasinometers@mic.com
		('2Y9yQbUpOnMQuEeLghoPEBu4orp1', -5), # skycasinometers@mic.com
		('eOM7mteC6gZ9yNlim5fWx08bmOq1', -5), # mohegansunmeters@mic.com
		('AXWCFEoN2ZSUiglB6DNJjHugwOg1', -8), # harrahssocal@mic.com
		('1LQMQ06VspVCs70ljzGBPIHHPgr1', -5), # lhmeters@mic.com
		('sygjkUWRD1bfpfQM8QQuzwNGQXx1', -5), # ybrmeters@mic.com
		('gK4CbuX1Jwa2ExZI0mmXRGnReZ03', -5), # ppmeters@mic.com
		('VX4QGsBEsAd5vPaNRfabmmGrA4f2', -5), # tsmeters@mic.com
		('yGHCFZbiZweOvblSGL1LOH9x4lf2', -5), # oneidameters@mic.com
		('by6NFwVisrVESJB04hyEBtZRgT03', -8), # timeters@mic.com
		('UIOUZogcnFPFH8S0j7RvLLuodkh1', -8), # autotest@mic.com
		('P14IjeLFv3Oyx4t2SHaG9IYULEu1', -8), # Christine.foster@everi.com
		('b8UzXzJGhQY6LtTuUPJNG3Y8ShW2', -8), # Subhasis.pradhan@everi.com
		('7rgeKrCE2NhjAWuZVcgVDwwOBoO2', -8), # paumameters@mic.com



		# ### For testing ###
		('1kyN8HCbC6gfZY8nNIYB1HjqRnH3', -8), # joe@capturemeters.com
		('xgdRnVu3yrgjEhrMQgDSImBEOCc2', -5)] # lotrrox@gmail.com

monthName = 'february2022'
yearName = '2022'

# Reset time is 5pm local
startDate = datetime.datetime(2022, 1, 31, 17, 0, tzinfo=timezone.utc)
endDate = datetime.datetime(2022, 3, 1, 17, 0, tzinfo=timezone.utc)

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

	blob = bucket.blob(uid + '/' + monthName + '.csv')
	blob.upload_from_filename('/Users/alexanderpowell/Desktop/projects/mic-monthly-report-generator/' + fileName)
	print('Report created for uid: ' + uid)

print(str(counter) + ' rows fetched in total')


















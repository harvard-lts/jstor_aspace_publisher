from xml.dom import minidom
import os, sys
import pymongo
from datetime import datetime

#uncomment below if you are running script outside docker (requires dotenv)
#from dotenv import load_dotenv
#load_dotenv()
mongo_url = os.environ.get('MONGO_URL')
mongo_database =  os.environ.get('MONGO_DBNAME')
mongo_collection =  'jstor_published_records'
field_name = 'record_id'

#gte_date = datetime(2023, 6, 15)
if len(sys.argv) != 4:
    print("Usage: python script.py <year> <month> <day>")
    sys.exit(1)

year = int(sys.argv[1])
month = int(sys.argv[2])
day = int(sys.argv[3])
gte_date = datetime(year, month, day)

client = pymongo.MongoClient(mongo_url, maxPoolSize=1)
db = client[mongo_database]
collection = db[mongo_collection]

results = collection.find({"harvest_date":{"$gte": gte_date},"status":"delete"}, {"record_id": 1})

unique_values = set()

for doc in results:
    field_value = doc.get(field_name)
    if field_value is not None:
        unique_values.add(field_value)

root = minidom.Document()
rootelem = root.createElement('ino:request')
rootelem.setAttribute('xmlns:ino', 'http://namespaces.softwareag.com/tamino/response2')
root.appendChild(rootelem)

for value in unique_values:
    ino_obj = root.createElement('ino:object')
    rootelem.appendChild(ino_obj)

    viaRecord = root.createElement('viaRecord')
    ino_obj.appendChild(viaRecord)

    recordId = root.createElement('recordId')
    recordId_text = root.createTextNode(value)
    recordId.appendChild(recordId_text)
    viaRecord.appendChild(recordId)

    deleted = root.createElement('deleted')
    deleted_text = root.createTextNode('Y')
    deleted.appendChild(deleted_text)
    viaRecord.appendChild(deleted)

xml_str = root.toprettyxml(indent='    ')

print(xml_str)
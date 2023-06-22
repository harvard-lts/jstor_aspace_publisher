from xml.dom import minidom
import os, sys
import pymongo
from datetime import datetime
#from dotenv import load_dotenv

#load_dotenv()
# MongoDB connection settings
mongo_url = os.environ.get('MONGO_URL')
mongo_database =  os.environ.get('MONGO_DBNAME')
mongo_collection =  'jstor_published_records'
# Field to retrieve from the documents
field_name = 'record_id'

#gte_date = datetime(2023, 6, 15)
# Check if all three arguments are provided
if len(sys.argv) != 4:
    print("Usage: python script.py <year> <month> <day>")
    sys.exit(1)

# Extract the command-line arguments
year = int(sys.argv[1])
month = int(sys.argv[2])
day = int(sys.argv[3])
gte_date = datetime(year, month, day)

# Connect to MongoDB
client = pymongo.MongoClient(mongo_url, maxPoolSize=1)
db = client[mongo_database]
collection = db[mongo_collection]

# Query the collection and retrieve the field from documents
results = collection.find({"harvest_date":{"$gte": gte_date},"status":"delete"}, {"record_id": 1})
#results = collection.distinct("record_id", {"harvest_date":{"$gte": gte_date}})

unique_values = set()

# Iterate over the query results
for doc in results:
    field_value = doc.get(field_name)
    if field_value is not None:
        unique_values.add(field_value)

# Create the root element
root = minidom.Document()
rootelem = root.createElement('ino:request')
rootelem.setAttribute('xmlns:ino', 'http://namespaces.softwareag.com/tamino/response2')
root.appendChild(rootelem)

for value in unique_values:
    # Create the next element
    ino_obj = root.createElement('ino:object')
    rootelem.appendChild(ino_obj)

    # Create the elemone element
    viaRecord = root.createElement('viaRecord')
    ino_obj.appendChild(viaRecord)

    # Create the recid element with the unique value
    recordId = root.createElement('recordId')
    recordId_text = root.createTextNode(value)
    recordId.appendChild(recordId_text)
    viaRecord.appendChild(recordId)

    # Create the delete element
    deleted = root.createElement('deleted')
    deleted_text = root.createTextNode('Y')
    deleted.appendChild(deleted_text)
    viaRecord.appendChild(deleted)

# Generate the XML string
xml_str = root.toprettyxml(indent='    ')

# Print or save the XML string
print(xml_str)
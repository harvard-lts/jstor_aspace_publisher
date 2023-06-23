#!/usr/local/bin/python3
#
# Run the script with it's -h option to see it's description
# and usage or scroll down at bit
#
# TME  06/11/18  Initial version
# TME  07/09/19  Made project an optional argument
#                Using SafeLoader with yaml module
# TME  10/19/22  Runs the complete process now

#
# Export modules, set/initialize global variables, grab arguments & check usage
#
import argparse, gzip, os, re, shutil, sys, tarfile, yaml
from glob import glob

# To help find other directories that might hold modules or config files
binDir    = os.path.dirname(os.path.realpath(__file__))
commonBin = binDir.replace('via/bin', 'bin')
libDir    = binDir.replace('via/bin', '/lib')

# Load any needed LTS and Alma modules
sys.path.append(commonBin)
sys.path.append(libDir)
from ltstools import get_date_time_stamp
from send_files import send_files
from notify import notify

confDir    = binDir.replace('bin', 'conf')
logDir     = binDir.replace('bin', 'log')
reRecordId = re.compile('.+deleteRecordId.*\>(\w+\d+)\<\/deleteRecordId.+')

usageMsg  = """
Send VIA export files to Primo and Shared Shelf. Full exports will be chunked.
Incremental exports will include deleted records.
Configuration files in the adjacent directory are used.
"""

# Check for any command line parameters
parser = argparse.ArgumentParser(description=usageMsg)
parser.add_argument("export", choices=['incr', 'full'], help="incr (incremental) or full Export")
parser.add_argument("-p", "--project", choices=['lc', 'primo'], help="lc (Library Cloud) or primo")
parser.add_argument("-f", "--file", help="full filename", required=False)
parser.add_argument("-j", "--jobid", help="job id of export", required=False)
parser.add_argument("-v", "--verbose", action = 'store_true', help = "Run with verbose output")
args = parser.parse_args()

export   = args.export
verbose  = args.verbose
notifyJM = False
fullFilename = args.file
jobTicketId = args.jobid


if args.project:
	confFile = f'{confDir}/via_{export}_{args.project}_export.yaml'
	jobName = f'VIA {export} {args.project} export'
else:
	confFile = f'{confDir}/via_{export}_export.yaml'
	jobName  = f'VIA {export} export'

jobCode        = f'via_export_{export}'
dateStamp      = get_date_time_stamp('day')
delExportFile  = f'via_export_del_{dateStamp}.xml'
chunkedExports = "viaExport_20*_[0-9][0-9][0-9].xml"

if args.file:
	fullExportFile = f'viafull_{fullFilename}.xml'
else:
	fullExportFile = f'viafull_{dateStamp}.xml'

#
# Main program
#
def run_main():
	global notifyJM
	sendExport = True
	
	# Create a notify object, this will also set-up logging
	logFile   = f'{logDir}/{jobCode}.{dateStamp}'
	notifyJM  = notify('log', jobCode, logFile)

	# Let the Job Monitor know that the job has started
	notifyJM.log('pass', jobName, verbose)
	notifyJM.report('start')

	# Open and loaded config file into an array of hashes
	if os.path.isfile(confFile):
		with open(confFile, 'r') as ymlfile:
			configSets = yaml.load(ymlfile, Loader=yaml.SafeLoader)
	else:
		notifyJM.log('fail', f'Configuration file {confFile} not found', True)
		notifyJM.report('stopped')
		quit()

	# Chunk files if export is full
	#if export == 'full':
	#	sendExport = chunk_full_export(configSets)

	# Compile and packup an xml file for any deleted records
	if export == 'incr':
		prep_incr_export(configSets, dateStamp)

	# Send files to remote systems
	if sendExport:
		[msgPass, msgWarn, msgFail] = send_files(confFile, 'sendFile', 'ALL')

		if msgFail:	
			notifyJM.log('fail', msgFail, True)
			print(msgFail)
			return -1
		if msgWarn:	
			notifyJM.log('warn', msgWarn, verbose)
			print(msgWarn)
		if msgPass: 
			notifyJM.log('pass', msgPass, verbose)
			print(msgPass)
			return 0

	notifyJM.report('complete')

#
# Sub-routines
#

def chunk_full_export(configSets):
	global notifyJM
	xmlHeader	   = '<viaCollection>'
	xmlFooter      = '</viaCollection>'
	recordCount    = 0
	recordsPerFile = 20000
	doneFlag       = 'done.FLAG'
	
	# Get export directory from config sets
	for configSet in configSets:
		try:
			profileName = configSet['profile_name']
		except:
			notifyJM.log('fail', 'Configuration file %s not formatted properly' % (confFile), True)
			return False
		try:
			exportDir = configSet['local_directory']
		except:
			notifyJM.log('fail', f'Configuration error: local_directory is not set in {confFile} for {profileName}', True)
			return False

	# We'll work in the export directory
	try:
		os.chdir(exportDir)
	except:
		notifyJM.log('fail', f'chdir {exportDir} Failed', True)
		
	# Clean-up from the previously export chunking
	notifyJM.log('info', 'Removing any old export files', verbose)
	for oldFile in glob(chunkedExports):
		os.remove(oldFile)
	for oldFile in glob('*.tar.gz'):
		os.remove(oldFile)
	try:
		os.remove(doneFlag)
	except:
		pass
		
	# Chunk today's full export if found
	if os.path.isfile(fullExportFile) and os.path.getsize(fullExportFile) > 0:
		notifyJM.log('info', f'Found {exportDir}/{fullExportFile}. Start chunking.', verbose)
		notifyJM.log('info', f'processing full set export for job ticket {jobTicketId}.', verbose)
		output = None
		fileCount = 0
		with open(fullExportFile) as input:
			for line in input:
			
				# Start of a record
				if '<viaRecord ' in line:
				
					# First record, start a new file (the first)
					if recordCount == 0:
						fileCount = 1
						output    = start_new_file(fileCount, xmlHeader)
						
					# Finish current file and then start a new one
					# when we hit our record per file max
					elif recordCount % recordsPerFile == 0:
						output.write(f'{xmlFooter}\n')
						output.close()
						fileCount += 1
						output    = start_new_file(fileCount, xmlHeader)
					
				# End of record, increment record count
				elif '</viaRecord>' in line:
					recordCount += 1
				
				# Save xml header, it will need to be added to each file
				elif '<viaCollection>' in line:
					xmlHeader = line.rstrip()
					continue

				# Write out current line
				if (output != None):
					output.write(line)
									
		notifyJM.log('pass', f'{recordCount} {jobName} records were chunked into {fileCount} files', verbose)
				
		# Pack up (tar and gzip) export files
		try:
			tarfileName = f'viafull_{dateStamp}.tar'
			if (jobTicketId != None):
				tarfileName = f'viafull_{jobTicketId}_{dateStamp}.tar'

			with tarfile.open(tarfileName, "w") as tarFile:
				for newFile in glob(chunkedExports):
					tarFile.add(newFile)

			gzippedFile = f'{tarfileName}.gz'
			with open(tarfileName, 'rb') as input:
				with gzip.open(gzippedFile, 'wb') as output:
					shutil.copyfileobj(input, output)

			os.remove(tarfileName)
			
		except Exception as error:
			notifyJM.log('fail', f'Failed to tar and gzip full export files. Error was: {error}', True)
			return False
				
		notifyJM.log('pass', f'Full records packed up in {gzippedFile}', verbose)			

	else:
		notifyJM.log('warning', f'{exportDir}/{fullExportFile} not found or empty', verbose)

	return True

# Compile an xml file for any deleted records.
# Xml file will then be tar and gzipped.
def prep_incr_export(configSets, dateStamp):
	global delExportFile, notifyJM
	
	# Might be used in directory name
	year  = dateStamp[0:4]
	month = dateStamp[4:6]
	day   = dateStamp[6:8]

	# Loop through config sets and find any delete profiles that need prep
	for configSet in configSets:
		try:
			profileName = configSet['profile_name']
		except:
			notifyJM.log('fail', 'Configuration file %s not formatted properly' % (confFile), True)
			return False
	
		if 'Delete' not in profileName: continue

		try:
			if not configSet['run_prep']: continue
		except:
			continue
			
		# Set the directory holding today's deletes
		try:
			localDir = configSet['local_directory']
		except:
			notifyJM.log('fail', 'Configuration error: local_directory is not set in %s for %s' % (confFile, profileName), True)
			return False

		localDir = localDir.replace('_YEAR_', year).replace('_MONTH_', month).replace('_DAY_', day)
	
		if not os.path.isdir(localDir):
			notifyJM.log('warn', f'No deleted records found for {profileName}', verbose)
			continue

		# Open each export file and grab any record IDs it might contain
		recordIds = []
		os.chdir(localDir)
		delFilenamePattern = r'^\d{3,4}_.*\.xml$'
		for file in glob('*.xml'):
			if re.match(delFilenamePattern , file):
				with open(file) as input:
					for line in input:
						match = reRecordId.match(line)
						if match:
							recordIds.append(match.group(1))
							os.remove(file)
					
		# Write file of deleted records if any were found
		if len(recordIds) > 0:
			notifyJM.log('info', f'Found deleted record export files in {localDir}', verbose)
			with open(delExportFile, 'w') as output:
				output.write('<?xml version="1.0" encoding="UTF-8" ?>\n')
				output.write('<ino:request xmlns:ino="http://namespaces.softwareag.com/tamino/response2">\n')

				for recordId in recordIds:
					output.write('\t<ino:object>\n')
					output.write('\t\t<viaRecord>\n')
					output.write(f'\t\t\t<recordId>{recordId}</recordId>\n')
					output.write('\t\t\t<deleted>Y</deleted>\n')
					output.write('\t\t</viaRecord>\n')
					output.write('\t</ino:object>\n')
				
				output.write('</ino:request>\n')
				
			notifyJM.log('pass', f'{delExportFile} was compiled using record IDs %s' % ', '.join(recordIds), verbose)

			# Tar and gzip delete export file
			try:
				tarfileName = delExportFile.replace('xml', 'tar')				
				with tarfile.open(tarfileName, "w") as tarFile:
					tarFile.add(delExportFile)

				gzippedFile = f'{tarfileName}.gz'
				with open(tarfileName, 'rb') as input:
					with gzip.open(gzippedFile, 'wb') as output:
						shutil.copyfileobj(input, output)
						
			except Exception as error:
				notifyJM.log('fail', f'Failed to tar and gzip {delExportFile}. Error was: {error}', True)
				
			os.remove(tarfileName)
			notifyJM.log('pass', f'Deleted records packed up in {tarfileName}', verbose)			

		else:
			notifyJM.log('warn', 'No delete records were exported', True)
			return False

	return True

# Start new export file using three digit file count
def start_new_file(fileCount, xmlHeader):

	fileCount3digit  = str(fileCount).zfill(3)
	chunkedExportOut = f"viaExport_{dateStamp}_{fileCount3digit}.xml"
	output = open(chunkedExportOut, 'w')
	output.write(f'{xmlHeader}\n')
	return output
	
#    
# Run main script
if __name__ == '__main__':
	exitcode = run_main()
	sys.exit(exitcode)

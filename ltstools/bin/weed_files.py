#!/usr/local/bin/python3 
#
# TME  07/17/17  Initial version
# TME  09/13/17  Added more error checking and weeding restrictions
# TME  03/05/18  Modified to report to the Job Monitor
# TME  03/15/18  Updated for Python 3
# TME  09/05/18  Renamed config file to weed_files.yaml 
#                and check config file parameter to '-c'
# TME  07/26/22  Using notify class. Report name of host system. Gzipping files
#                now supported. Weeding based on file count now supported.
# TME  09/14/22  Using SafeLoader with yaml module
# TME  10/04/22  Tar and zips directories

#
# Load modules, define variables, grab arguments & check usage
#
import argparse, gzip, os, sys, time, re, shutil, tarfile, yaml
from socket import gethostname

# To help find other directories that might hold modules or config files
binDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
commonLib = binDir.replace('bin', 'lib')
sys.path.append(commonLib)
from ltstools import get_date_time_stamp
from notify import notify

jobName      = 'Weed Files'
jobCode      = 'weed_files'
confDir      = binDir.replace('bin', 'conf')
confFile     = os.path.join(confDir, 'weed_files.yaml')
logDir       = binDir.replace('bin', 'log')
reZipFile    = re.compile('.+\.gz$')
reTarZipFile = re.compile('.+\.tgz$')

# Do not weed the following directories (but their sub-directories are allowed)  
dirsNotAllowed = ('/', '/bin', '/boot', '/dev', '/drs', '/easdata', '/etc', '/lib', '/lib64', '/lost+found', '/proc', '/sbin', '/selinux', '/sys', '/usr') 

usageMsg = f'Weed files using configuration profiles defined in {confFile}'

# Check for any command line parameters
parser = argparse.ArgumentParser(description=usageMsg)
group = parser.add_mutually_exclusive_group()
group.add_argument("-n", "--noremove", action='store_true', help="find files to weed but do Not remove or zip them")
group.add_argument("-c", "--checkconf", action='store_true', help="Check configuration file, no files are copied")
parser.add_argument("-v", "--verbose", action = 'store_true', help = "Run with verbose output")
args = parser.parse_args()

noRemove  = args.noremove
checkConf = args.checkconf
verbose   = args.verbose

fileRmCount  = 0
fileZipCount = 0
secondsNow   = time.time()            # used to check a file or directory "age"
notifyJM     = False

#
# Main program
#
def main():
	global fileRmCount, notifyJM, verbose

	# Start job monitoring and logging
	logFile = f'{logDir}/{jobCode}_' + get_date_time_stamp('month')

	if checkConf:
		verbose  = True
		notifyJM = notify('echo', jobCode)
		notifyJM.log('pass', f'Check configuration file {confFile}', verbose)
	else:
		notifyJM = notify('monitor+log', jobCode, logFile)
		notifyJM.log('pass', 'Weeding all defined config sets on %s' % gethostname(), verbose)
		notifyJM.report('start')

	# Load config table
	try:
		with open(confFile, 'r') as ymlfile:
			configTable = yaml.load(ymlfile, Loader=yaml.SafeLoader)
	except:
		notifyJM.log('fail', f"Failed to read {confFile}", verbose)
		quit()

	# Weed files for each profile in config table (or just check config)
	for configSet in configTable:
		badConfigSet = False
		fileRmCount = 0

		# Check, get, and set parameters
		try:
			confProfile = configSet['profile_name']
		except:
			confProfile = False
		try:
			searchCriteria = configSet['search_criteria']
		except:
			searchCriteria = False
		try:
			regularExpression = configSet['regular_expression']
		except:
			regularExpression = False
		try:
			searchDir = configSet['search_dir']
		except:
			searchDir = False
		try:
			removeOldFiles = configSet['remove_old_files']
		except:
			removeOldFiles = False
		try:
			daysToKeep = configSet['days_to_keep']
		except:
			daysToKeep = False
		try:
			filesToKeep = configSet['files_to_keep']
		except:
			filesToKeep = False
		try:
			zipOldFiles = configSet['zip_old_files']
		except:
			zipOldFiles = False
		try:
			daysToKeepUnzipped = configSet['days_to_keep_unzipped']
		except:
			daysToKeepUnzipped = False

		if not confProfile:
			notifyJM.log('fail', f'Profile name is not set in {confFile} for config set', verbose)
			badConfigSet = True

		if not searchCriteria:
			notifyJM.log('fail', f'Search criteria is not set in {confFile} for {confProfile}', verbose)
			badConfigSet = True

		# Check that search directory is supported
		if searchDir:
			if searchDir in dirsNotAllowed:
				notifyJM.log('fail', f'The search directory {searchDir} set in {confFile} for {confProfile} is not supported', verbose)
				badConfigSet = True
		else:
			notifyJM.log('fail', f'Search Dir is not set in {confFile} for {confProfile}', verbose)
			badConfigSet = True

		# Don't allow regex search patterns that will match all files
		if regularExpression:
			if regularExpression == '^.*$' or regularExpression == '^.+$':
				notifyJM.log('fail', f'The regular expression search pattern {regularExpression} set in {confFile} for {confProfile} is not supported', verbose)
				badConfigSet = True
		else:
			notifyJM.log('fail', f'Regular Expression is not set in {confFile} for {confProfile}', verbose)
			badConfigSet = True
					
		# Convert retention period from days to seconds if we're removing files
		if removeOldFiles:
			if daysToKeep:
				secondsToKeep = daysToKeep * 86400
			elif not filesToKeep:
				notifyJM.log('fail', f'Neither retention Period or files to keep is set in {confFile} for {confProfile}', verbose)
				badConfigSet = True

		# And then do the same for leaving files unzipped
		if zipOldFiles:
			if daysToKeepUnzipped:
				secondsToKeepUnzipped = daysToKeepUnzipped * 86400
			else:
				notifyJM.log('fail', f'Period to keep file unzipped is not set in {confFile} for {confProfile}', verbose)
				badConfigSet = True

		if not removeOldFiles and not zipOldFiles:
			notifyJM.log('fail', f'No actions set in {confFile} for {confProfile}', verbose)
			badConfigSet = True
			continue

		if badConfigSet: continue
					
		notifyJM.log('pass', f'\n{confProfile}', verbose)

		# Do not actually weed files if we're only checking the config
		if checkConf:
			notifyJM.log('pass', 'profile looks good\n', verbose)
			continue
   
		# Check that search directory exist
		if not os.path.isdir(searchDir ):
			notifyJM.log('fail', f'The search directory {searchDir} set in {confFile} for {confProfile} does not exist', verbose)
			continue
		
		# Search for file or directories matching our search regex pattern
		reSearchPattern = re.compile(regularExpression)
		for parentDir, dirs, files in os.walk(searchDir):
			os.chdir(parentDir)

			# Handle the weeding of directories
			if searchCriteria == "DIRNAME":
				for directory in dirs:
					if reSearchPattern.match(directory):
						dirAge = secondsNow - os.stat(directory).st_mtime

						# Gzip old directories if specified
						if zipOldFiles:
							fileWasCompress = compress_dir(directory, parentDir, dirAge, secondsToKeepUnzipped)

						# Remove old directories if specified and wasn't zipped
						if removeOldFiles and not fileWasCompress:
							if dirAge > secondsToKeep:
								removeFileOrDir(searchCriteria, directory, parentDir)
					
			# Handle the weeding of files
			elif searchCriteria == "FILENAME":
				filesSorted = sorted(files, key = os.path.getmtime)
				filesSorted.reverse()
				fileFoundCount = 0
				
				for file in filesSorted:
					if reSearchPattern.match(file):
						fileAge = secondsNow - os.stat(file).st_mtime
						fileWasRemoved  = False
						fileWasCompress = False
												
						# Just keep a certain number of files
						if filesToKeep:
							fileFoundCount += 1	
							if fileFoundCount > filesToKeep:
								fileWasRemoved = removeFileOrDir(searchCriteria, file, parentDir)

						# Gzip old files
						if zipOldFiles and not fileWasRemoved:
							fileWasCompress = compress_file(file, parentDir, fileAge, secondsToKeepUnzipped)

						# Remove files based on it's date
						if daysToKeep and not fileWasCompress and not fileWasRemoved:
							if fileAge > secondsToKeep:
								removeFileOrDir(searchCriteria, file, parentDir)
							
		# Log results message
		if removeOldFiles:
			if searchCriteria == "DIRNAME":
				if noRemove:
					notifyJM.log('pass', f'{fileRmCount} directories found to remove', verbose)
				else:
					notifyJM.log('pass', f'{fileRmCount} directories removed', verbose)
			else:
				if noRemove:
					notifyJM.log('pass', f'{fileRmCount} files found to remove', verbose)
				else:
					notifyJM.log('pass', f'{fileRmCount} files removed', verbose)

		if zipOldFiles:
			if searchCriteria == "DIRNAME":
				if noRemove:
					notifyJM.log('pass', f'{fileZipCount} directories found to zip', verbose)
				else:
					notifyJM.log('pass', f'{fileZipCount} directories zipped', verbose)
			else:
				if noRemove:
					notifyJM.log('pass', f'{fileZipCount} files found to zip', verbose)
				else:
					notifyJM.log('pass', f'{fileZipCount} files zipped', verbose)

	# Notify Job Monitor or just the user
	notifyJM.report('complete')

#
# Functions
#

# Remove file or directory (unless noRemove is set)
def removeFileOrDir(searchCriteria, fileOrDir, parentDir):
	global fileRmCount, notifyJM
	fileWasRemoved = True
	fileRmCount   += 1

	if noRemove:
		notifyJM.log('pass', f"Should be removed: {parentDir}/{fileOrDir}", verbose)
	else:
		try:
			if searchCriteria == "DIRNAME":
				shutil.rmtree(fileOrDir)
			else:
				os.remove(fileOrDir)

			notifyJM.log('pass', f"Removed {parentDir}/{fileOrDir}", verbose)

		except:
			notifyJM.log('fail', f"Unable to removed {parentDir}/{fileOrDir}", verbose)
			fileWasRemoved = False
			fileRmCount   -= 1

	return fileWasRemoved

# Gzip file if its not already zipped and its older than its keep unzipped time
def compress_file(fileToCheck, parentDir, fileAge, secondsToKeepUnzipped):
	global fileZipCount, notifyJM
	fileWasCompress = False

	if not reZipFile.match(fileToCheck):
		if fileAge > secondsToKeepUnzipped:
			fileZipCount += 1

			# Do not zip anything if noRemove was specified
			if noRemove:
				notifyJM.log('pass', f"Should be zipped: {parentDir}/{fileToCheck}", verbose)
				fileWasCompress = True
			else:
				try:			
					gzippedFile = f'{fileToCheck}.gz'
					with open(fileToCheck, 'rb') as input:
						with gzip.open(gzippedFile, 'wb') as output:
							shutil.copyfileobj(input, output)
		
					os.remove(fileToCheck)
					notifyJM.log('pass', f"Gzipped {parentDir}/{fileToCheck}", verbose)
					fileWasCompress = True
 
				except:
					notifyJM.log('fail', f"Unable to gzip {parentDir}/{fileToCheck}", verbose)

	return fileWasCompress

# Tar and gzip directory if its not already and its older than its keep unzipped time
def compress_dir(dirToCheck, holdingDir, dirAge, secondsToKeepUnzipped):
	global fileZipCount, notifyJM
	dirWasCompress = False

	if not reTarZipFile.match(dirToCheck):
		if dirAge > secondsToKeepUnzipped:
			fileZipCount += 1

			# Do not zip anything if noRemove was specified
			if noRemove:
				notifyJM.log('pass', f"Should be Tar and gzipped: {holdingDir}/{dirToCheck}", verbose)
				dirWasCompress = True
			else:
				os.chdir(holdingDir)
				tarfileName = f'{holdingDir}/{dirToCheck}.tgz'				
				with tarfile.open(tarfileName, "w:gz") as tarFile:
					try:
						for parentDir, dirs, files in os.walk(dirToCheck):
					
							if len(dirs) > 0:
								for dir in dirs:
									tarFile.add(f'{parentDir}/{dir}')
									dirWasCompress = True
				
							if len(files) > 0:
								for file in files:
									tarFile.add(f'{parentDir}/{file}')
									dirWasCompress = True
					except:
						notifyJM.log('fail', f"Unable to tar and gzip {holdingDir}/{dirToCheck}", verbose)
										
				if dirWasCompress:
					tarFile.close()
					
					if os.path.isfile(tarfileName) and os.path.getsize(tarfileName) > 0:
						shutil.rmtree(f"{holdingDir}/{dirToCheck}")
						notifyJM.log('pass', f"Tar and gzipped {holdingDir}/{dirToCheck}", verbose)
					else:
						notifyJM.log('fail', f"Unable to tar and gzip {holdingDir}/{dirToCheck}", verbose)
						dirWasCompress = False
					 
	return dirWasCompress

#    
# Run script (main)
#
if __name__ == '__main__':
    main()

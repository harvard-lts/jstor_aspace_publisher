#!/usr/local/bin/python3
#
# Run the script with it's -h option to see it's description
# and usage or scroll down at bit
#
# TME  05/29/18  Initial version
# TME  06/11/18  Added support for today's date in local directory name
# TME  07/02/18  Renamed remote parameters to upload
# TME  07/11/18  Fixed ftp disconnect issue
# TME  07/17/18  Fixed message reporting
# TME  08/10/18  Using SFTP rather than SCP
#                Removed files to send parameter
#                Added datestamp keyword support for local files parameter
# TME  08/27/18  Fixed changing to remote directory problem
# TME  11/06/18  Unpack files before sending if needed and specified
# TME  11/29/18  Fixed disconnect bug (again:))
# TME  06/05/19  Using retrying.retry
# TME  09/27/19  Fixed ftp upload issues. Using SafeLoader with yaml module.
# TME  09/30/19  Catch any exceptions thrown while trying to untar files
# TME  06/26/20  Clean-up any unpacked files if needed
# TME  07/21/20  Catch any exceptions thrown while trying to gunzip files
# TME  03/05/21  Catch any exceptions thrown while trying disconnect from remote site
# TME  04/14/21  Added support for scp file transfers
# TME  05/11/21  scp_file() now using sshPort
# TME  06/23/21  File renaming before uploading is now supported
# TME  07/16/21  Can now specified ssh key to use for SCP
# TME  09/14/21  Support for a new config parameter to specify 
#                how to report when no files are found to upload

#
# Load modules, set/initialize global variables, grab arguments & check usage
#
import shutil, os, re, yaml, ftplib, pysftp, gzip, tarfile, shutil, stat
from subprocess import run, PIPE
from retrying import retry

scp = '/bin/scp'

ftpSession  = False
sftpSession = False

msgPass = ''
msgWarn = ''
msgFail = ''

# Used when unpacking files
reGzippedFiles = re.compile('.+\.gz$')
reTarredFiles = re.compile('.+\.tar$')

# run_script
# Checked usage, run main script and then display result
# Used when the script is called from the command prompt
def run_script():
	import argparse

	jobName  = 'Send Files'

	usageMsg  = """
	Send files to remote systems. A configuration file, with the needed 
	parameters, must be specified. See send_files.yaml_template in the 
	adjacent conf directory for an example.
	"""

	# Check for any command line parameters
	parser = argparse.ArgumentParser(description=usageMsg)
	parser.add_argument("conf_file", help="Configuration file to use")
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-p", "--profile", help="Run only the specified profile in configuration file. Otherwise, all are run.")
	group.add_argument("-c", "--checkconf", action='store_true', help="Check configuration file, no files are sent")
	args = parser.parse_args()

	# Required, config file
	confFile = args.conf_file

	if args.checkconf:
		mode     = 'checkConf'
		profiles = 'ALL'
	elif args.profile:
		mode     = 'sendFile'
		profiles = args.profile
	else:
		mode     = 'sendFile'
		profiles = 'ALL'

	# Call function and then catch and display results
	[msgPass, msgWarn, msgFail] = send_files(confFile, mode, profiles)

	print(jobName)

	if msgFail:
		print('Failed\n' + msgFail)
	if msgWarn:
		print('Warnings\n' + msgWarn)
	if msgPass:
		print('Successful\n' + msgPass)

# send_files
# Send files to remote systems.
#
# Parameters: confFile    Configuration file to use
#             mode        sendFile or checkConf
#             profiles    The keyword 'ALL' or a single profile name
#
# Returns:    An array containing msgPass, msgWarn and msgFail 
#             containing successful, warning and failure messages
#
def send_files(confFile, mode, profiles = 'ALL'):
	from datetime import datetime
	global ftpSession, sftpSession, msgPass, msgWarn, msgFail

	# Might use in file or directory names
	today    = datetime.now()
	yyyymmdd = today.strftime('%Y%m%d')
	year     = today.strftime('%Y')
	month    = today.strftime('%m')
	day      = today.strftime('%d')

	# Open and loaded config file into an array of hashes
	if os.path.isfile(confFile):
		with open(confFile, 'r') as ymlfile:
			configSets = yaml.load(ymlfile, Loader=yaml.SafeLoader)
	else:
		return [False, False, 'Configuration file %s not found' % (confFile)]

	profileFound  = False
	newFilesFound = False

	# Loop through config sets, check/set parameters
	for configSet in configSets:
		badConfigSet = False
		fileUploaded = False

		try:
			profileName = configSet['profile_name']
		except:
			profileName = False
		try:
			xferProtocol = configSet['upload_protocol']
		except:
			xferProtocol = False
		try:
			sshPort = configSet['upload_port']
		except:
			sshPort = 22
		try:
			sendDone = configSet['send_done']
		except:
			sendDone = False
		try:
			remoteSite = configSet['upload_site']
		except:
			remoteSite = False
		try:
			remoteUser = configSet['upload_user']
		except:
			remoteUser = False
		try:
			password = configSet['upload_password']
		except:
			password = False
		try:
			privateKey = configSet['private_key']
		except:
			privateKey = False
		try:
			remoteDir = configSet['upload_directory']
		except:
			remoteDir = False
		try:
			localDir = configSet['local_directory']
		except:
			localDir = False
		try:
			localFiles = configSet['local_files']
		except:
			localFiles = False
		try:
			renameUploadFile = configSet['rename_upload_file']
		except:
			renameUploadFile = False
		try:
			localArchive = configSet['local_archive']
		except:
			localArchive = False                        
		try:
			handleUnmatchedFiles = configSet['handle_unmatched_files']
		except:
			handleUnmatchedFiles = False                        
		try:
			unpackFiles = configSet['unpack_first']
		except:
			unpackFiles = False                        
		try:
			handleNoFiles = configSet['handle_no_files']
		except:
			handleNoFiles = 'PASS'                        
	   
		# Just send files for a single config profile
		if profileName:        
			if not profiles == 'ALL':
				if not profileName == profiles:
					continue
		else:
			msgFail += 'Configuration error: profile_name is not set in %s\n' % (confFile)
			badConfigSet = True
		
		if not xferProtocol:
			msgFail += 'Configuration error: upload_protocol is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if not remoteSite:
			msgFail += 'Configuration error: upload_site is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if not remoteUser:
			msgFail += 'Configuration error: upload_user is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if not remoteDir:
			msgFail += 'Configuration error: upload_directory is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if not localDir:
			msgFail += 'Configuration error: local_directory is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if not localFiles:
			msgFail += 'Configuration error: local_files is not set in %s for %s\n' % (confFile, profileName)
			badConfigSet = True
		if xferProtocol == 'FTP':
			if not password:
				msgFail += 'Configuration error: password is not set in %s for %s\n' % (confFile, profileName)
				badConfigSet = True
	
		if badConfigSet: continue

		# Just check config file if specified, no files are copied
		if mode == 'checkConf':
			print('Profile %s looks good' % profileName)
			continue

		profileFound = True

		# Make sure that we don't already have a s/ftp connection
		if sftpSession: sftp_disconnect()
		if ftpSession: ftp_disconnect()

		# Swap any key words
		localDir   = localDir.replace('_YEAR_', year).replace('_MONTH_', month).replace('_DAY_', day)
		localFiles = localFiles.replace('_YEAR_', year).replace('_MONTH_', month).replace('_DAY_', day)

		# Check that all the needed directories exist
		if not os.path.exists(localDir):
			msgFail += 'Error: local directory %s not found\n' % (localDir)
			continue
		if localArchive:
			if not os.path.exists(localArchive):
				msgFail += 'Error: archive directory %s not found\n' % (localArchive)
				continue

		# Check for local files that we want to upload
		reLocalFiles = re.compile(localFiles)
		os.chdir(localDir)
		for localFile in os.listdir('.'):
	
			match = reLocalFiles.match(localFile)
		
			# Track unmatched files if specified by config set
			if match == None:
				if handleUnmatchedFiles:
					msg = '%s found in %s but does not match the file name regular expression of %s\n' % (localFile, localDir, localFiles)
					if handleUnmatchedFiles == 'FAIL':
						msgFail += msg
					elif handleUnmatchedFiles == 'WARN':
						msgWarn += msg
				continue

			newFilesFound = True

			# Set new name for file if renaming was specified
			if renameUploadFile:
				newFileName = renameUploadFile.replace('_MATCH_', match.group(1))
		
			# Unpack files if specified and needed
			if unpackFiles:
				unzippedFile = False
				untarredFile = False

				# Ungzip if needed
				match = reGzippedFiles.match(localFile)
				if match != None:
					unzippedFile = localFile.replace('.gz', '')
					if not os.path.isfile(unzippedFile):					
						try:
							with gzip.open(localFile, 'rb') as fileIn:
								with open(unzippedFile, 'wb') as fileOut:
									shutil.copyfileobj(fileIn, fileOut)
						except:
							msgFail += "Failed to gunzip %s/%s\n" % (localDir, localFile)
							continue
						
					localFile = unzippedFile

				# Extract tar archive if needed
				match = reTarredFiles.match(localFile)
				if match != None:
					untarredFile = localFile.replace('.tar', '')
					if not os.path.isfile(untarredFile):
						try:
							tar = tarfile.open(localFile)
							tar.extractall()
							tar.close()
						except:
							msgFail += "Failed to untar %s/%s\n" % (localDir, localFile)
							continue

					localFile = untarredFile
				
					# Make sure file permissions are correct (Alma has issues)
					os.chmod(localFile, stat.S_IRUSR)

			# Use new name when uploading if renaming was specified
			if renameUploadFile:
				remoteFile = f'{remoteDir}/{newFileName}'
				
			# Otherwise, just use local file name
			else:
				remoteFile = remoteDir + '/' + localFile

			localFileFullPath = os.path.join(localDir, localFile)
 
			if xferProtocol == 'FTP':
				fileUploaded = ftp_file(remoteSite, remoteUser, password, remoteFile, localFileFullPath)
			elif xferProtocol == 'SFTP':
				fileUploaded = sftp_file(remoteSite, remoteUser, password, privateKey, sshPort, remoteFile, localFileFullPath)
			elif xferProtocol == 'SCP':
				fileUploaded = scp_file(remoteSite, remoteUser, privateKey, sshPort, remoteFile, localFileFullPath)
			
			if fileUploaded:
				msgPass += '%s %s to %s:%s\n' % (localFileFullPath, xferProtocol, remoteSite, remoteFile)
				fileUploaded = False
			else:
				continue
	
			# Archive upload file if specified
			if localArchive:

				# Archive using new name if renaming was specified
				if renameUploadFile:
					archiveFileFullPath = f'{localArchive}/{newFileName}'
				
				# Otherwise, just use local file name
				else:
					archiveFileFullPath = f'{localArchive}/{localFile}'

				try:
					shutil.copy(localFileFullPath, archiveFileFullPath)
				except:
					msgFail += "Failed to copy %s to %s\n" % (localFileFullPath, archiveFileFullPath)
					continue
				try:
					os.remove(localFileFullPath)
				except:
					msgFail += "Failed to remove %s\n" % (localFileFullPath)
					continue

			# If files were unpacked, clean up
			if unpackFiles:
				if unzippedFile: os.remove(unzippedFile)
				if untarredFile: os.remove(untarredFile)

	if mode == 'checkConf':
		if msgFail:
			msgPass += "%s is formatted properly" % (confFile)

	elif not profileFound:
		msgWarn += 'No matching config profiles were found\n'
		
	elif not newFilesFound:
		if handleNoFiles == 'FAIL':
			msgFail += 'No new files found\n'
		elif handleNoFiles == 'WARN':
			msgWarn += 'No new files found\n'
		else:
			msgPass += 'No new files found\n'
	
	# pysftp does not clean-up after itself very well
	if sftpSession: sftp_disconnect()

	# Return results 
	return [msgPass, msgWarn, msgFail]

# Upload file using SFTP
def sftp_file(remoteSite, remoteUser, password, privateKey, sshPort, remoteFile, localFile):
	global msgFail

	# Connect to sftp site if we're not already
	if not sftpSession:
		try:
			rc = sftpConnect(remoteSite, remoteUser, password, privateKey, sshPort)        	
			if not rc: return rc
		except:
			msgFail += "Login to %s using %s account\n" % (remoteSite, remoteUser)
			return False

	# And then upload file
	try:
		sftpSession.put(localFile, remoteFile)
	except:
		msgFail += "Sftp %s to %s@%s:%s\n" % (localFile, remoteUser, remoteSite, remoteFile)
		return False

	return True
    
# Connect to SFTP server
# Multiple attempts are made if needed
# Start with a 10 second delay between tries
@retry(stop_max_attempt_number = 5, wait_exponential_multiplier = 10000, wait_exponential_max = 100000)
def sftpConnect(remoteSite, remoteUser, password, privateKey, sshPort):
	global sftpSession, msgFail

	if password:
		sftpSession = pysftp.Connection(host=remoteSite,username=remoteUser,private_key=None,password=password,port=sshPort)
	elif privateKey:
		sftpSession = pysftp.Connection(host=remoteSite,username=remoteUser,private_key=privateKey,port=sshPort)
	else:
		msgFail += "No password or private key specified for %s at %s\n" % (remoteUser, remoteUser)
		return False
		
	return True

# Disconnect and quit SFTP session
def sftp_disconnect():
    global sftpSession
    
    if sftpSession:
        sftpSession.close()
        sftpSession = False

    return True
    
# Upload file using FTP
def ftp_file(remoteSite, remoteUser, password, remoteFile, localFile):
	global msgFail, ftpSession

	# Connect to ftp site if we're not already
	if not ftpSession:
		try:
			ftsSession = ftpConnect(remoteSite, remoteUser, password)
			if not ftpSession:
				msgFail += "Login to %s using %s account\n" % (remoteSite, remoteUser)
				return False
		except:
			msgFail += "Login to %s using %s account\n" % (remoteSite, remoteUser)
			return False
	
	# Open file and then send it
	try:
		file = open(localFile,'rb')
		ftpCmd = 'STOR %s' % remoteFile
		ftpSession.storbinary(ftpCmd, file)     # send the file
		file.close()                            # close file and FTP
	except:
		msgFail += "FTP %s to %s using the %s account\n" % (localFile, remoteSite, remoteUser)
		return False

	return True
    
# Connect to FTP server
# Multiple attempts are made if needed
# Start with a 10 second delay between tries
@retry(stop_max_attempt_number = 5, wait_exponential_multiplier = 10000, wait_exponential_max = 100000)
def ftpConnect(remoteSite, remoteUser, password):
	global ftpSession
	
	ftpSession = ftplib.FTP(remoteSite, remoteUser, password)
	return ftpSession

# Disconnect and quit FTP session
def ftp_disconnect ():
	global ftpSession

	if ftpSession:
		try:
			ftpSession.quit()
			ftpSession = False
		except:
			pass

	return True

# Upload file using SCP
def scp_file(remoteSite, remoteUser, privateKey, sshPort, remoteFile, localFile):
	global msgFail

	destination = f'{remoteUser}@{remoteSite}:{remoteFile}'
	
	command = scp
	if sshPort: command += f' -P {sshPort}'
	if privateKey: command += f' -i {privateKey}'
	command += f' {localFile} {destination}'

	try:
		cp = run(command.split(), stdout=PIPE, universal_newlines=True)
	except:
		msgFail += f"Failed {command}\n"
		return False

	if cp.returncode > 0:
		if cp.stderr:
			msgFail += f"Failed: {command}. Error was: cp.stderr\n"
		else:
			msgFail += f"Failed: {command}\n"
		return False
	else:
		return True

#    
# Run script, with usage check, if called from the command prompt 
#
if __name__ == '__main__':
    run_script()

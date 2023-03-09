#!/bin/env python3
#
# Run the script with it's -h option to see it's description
# and usage or scroll down at bit
#
# TME  02/16/18  Initial version
# TME  03/19/18  Updated for Python 3
# TME  04/16/19  Added retries for Job Monitor notification
# TME  03/24/21  Delays between retries grow longer. Added option to disable
#                retries. Messages that exceed size limt are written to disk.
# TME  03/30/21  Use utf-8 if ascii fails when encoding before checking message size
# TME  11/19/21  Max allowed message size lowered to 65535 since that's 
#                all that the database field can hold
# TME  09/28/22  Increased max allowed message size to 204800
# TME  11/30/22  Returned max allowed message size to 65535

#
# Load modules, set/initialize global variables
#
import os, shutil, sys
from time import sleep

# To help find other directories that might hold modules or config files
binDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
scriptLib = binDir.replace('bin', 'lib')
logDir    = binDir.replace('bin', 'log')
sys.path.append(scriptLib)
from ltstools import adminMailTo, adminMailFrom, get_date_time_stamp, jobMonitor, send_mail

usageMsg  = """
Notify the Job Monitor. Supported status codes are;

		 STARTED_SUCCESS
		 STARTED_WARNING
		 STARTED_FAIL
		 RUNNING
		 RUNNING_WARNING
		 RUNNING_ERROR
		 FAILED
		 COMPLETED_SUCCESS
		 COMPLETED_WARNING
		 COMPLETED_FAILED
"""

# Number of attempts to notify the Job Monitor
maxTries  = 6

# Wait, in seconds, between tries. It will be doubled with each retry.
retryWait = 60

# run_script
# Checked usage, run main script and then display result
# Used when the script is called from the command prompt
def run_script():
    import argparse
    
    jobName  = 'Notify Job Monitor'

    # Check for any command line parameters
    parser = argparse.ArgumentParser(description=usageMsg)
    parser.add_argument("job_code", help = "Job code as defined in the Job Monitor")
    parser.add_argument("status_code", help = "Status code as defined in the Job Monitor (see above)")
    parser.add_argument("-m", "--message", help = "Job result message to pass to the Job Monitor")
    parser.add_argument("-r", "--run_id", help = "Job run ID used to identify the job run in the Job Monitor")
    parser.add_argument("-n", "--no_retries", action = 'store_true', help = "Do not retry if Job Monitor fails to respond")
    args = parser.parse_args()

    checkConf     = False
    configProfile = False
    configGroup   = False

    # Required, job code and status code
    jobCode    = args.job_code
    statusCode = args.status_code

    # Optional, message and run ID
    if args.message:
        message = args.message
    else:
        message = 'none'
    if args.run_id:
        runId = args.run_id
    else:
        runId = False
        
    noRetries = args.no_retries

    # Call function and then catch and display results
    returnMsg = notifyJM(jobCode, statusCode, message, runId, noRetries)

    print(returnMsg)

# notifyJM
# Notify Job Monitor
#
# Parameters
#   jobCode       Job Monitor job code such as "edi_orders" or "plif"
#
#   statusCode    Supported Job Monitor status codes are;
#                     STARTED_SUCCESS
#                     STARTED_WARNING
#                     STARTED_FAIL
#                     RUNNING
#                     RUNNING_WARNING
#                     RUNNING_ERROR
#                     FAILED
#                     COMPLETED_SUCCESS
#                     COMPLETED_WARNING
#                     COMPLETED_FAILED
#
#   message       Optional, use to pass a result message to the Job Monitor
#
#   runId         Optional, use to update a specific Job Monitor job run.
#                 This parameter is not usually needed.
#
#   noRetries     Do not retry if Job Monitor fails to respond
#
def notifyJM(jobCode, statusCode, message = 'none', runId = False, noRetries = False):
	from urllib import request
	global retryWait
	httpError = False

	if runId:
		notifyJmUrl = '%s/set_job_status/job_code/%s/status_code/%s/run_id/%s' % (jobMonitor, jobCode, statusCode, runId)
	else:
		notifyJmUrl = '%s/set_job_status/job_code/%s/status_code/%s' % (jobMonitor, jobCode, statusCode)

	try:
		msgEncoded = message.encode('ascii')
	except:
		msgEncoded = message.encode('utf-8')
	
	# If message is too large, write it to disk and report it
	msgSize = len(msgEncoded)
	if msgSize > 65535:
		dateStamp = get_date_time_stamp()
		logFile = f'{logDir}/{jobCode}{dateStamp}.log'
		with open(logFile, 'w') as log:
			log.write(message)
			
		from socket import getfqdn
		hostname = getfqdn()
		message = f'The results message was {msgSize} bytes. That exceeds the size limitation of 65535 bytes so the message was written to {hostname}:{logFile}.\n'
		print(message)
		msgEncoded = message.encode('utf-8')
		
	# Post status to the Job Monitor. Multiple attempts might be made.
	for loopCount in range(1, (maxTries + 1)):
		try:
			post = request.Request(url = notifyJmUrl, data = msgEncoded)         
			response = request.urlopen(post)
			httpStatus = response.read().decode('utf-8')
			[runId, httpRc] = httpStatus.replace(' ', '').split(',')
			
			if httpRc == '200':
				httpError = False
				break
			else:
				httpError = True

		except Exception as e:
			httpError = e

		# Retry unless asked not to
		if not noRetries:
			if loopCount < maxTries:
				print(f'The Job Monitor did not respond. Another attempt will be made after in {retryWait} seconds.')
				sleep(retryWait)
				retryWait += retryWait

	if httpError:
			mailTo   = adminMailTo
			mailFrom = adminMailFrom
			message  = 'Failed to notify the Job Monitor with a url of %s\n' % notifyJmUrl
			message += 'Http error was %s.' % (httpError)
			send_mail(mailTo, mailFrom, 'Failed to notify the Job Monitor', message)
			return message

	return runId
    
#    
# Run script, with usage check, if called from the command prompt 
#
if __name__ == '__main__':
    run_script()

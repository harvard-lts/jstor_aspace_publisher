# Use this module for script reporting
#
# TME  11/06/18  Initial version
# TME  06/06/19  Use message type headings if more than one type of message is reported
# TME  08/15/19  Added support logging with the 'monitor+log'
# TME  03/27/20  Logging now appends to log file
# TME  04/08/20  Added log notify method to log without notifying the
#                Job Monitor. Also added status message counters.
# TME  04/30/20  Don't try to print job code when echo is set
# TME  06/16/20  report() now accepts a header message
# TME  01/24/22  Use FAILED rather than FAIL for status

# To load notifyJM so that we can report to the Job Monitor
import logging, os, sys
commonLib = os.path.dirname(os.path.realpath(__file__))
commonBin = commonLib.replace('lib', 'bin')
sys.path.append(commonBin)
from notifyJM import notifyJM

# Use this class to track pass, fail and warning script messages and
# to report script results. 
class notify:

	def __init__(self, notifyMethod, jobCode = False, logFile = False):

		if notifyMethod == 'monitor' or notifyMethod == 'monitor+log':
			if jobCode:
				self.jobCode = jobCode
			else:
				print('A job code must be set to report to the Job Monitor')
				return None

		# Set up logging if specified. Path to a log file must be passed.
		if notifyMethod == 'monitor+log' or notifyMethod == 'log':
			if logFile:
				self.logFile = logFile
			else:
				print('A path to a log file must be set to log messages')
				return None

			logging.basicConfig(level=logging.INFO,
								format='%(asctime)s %(levelname)s %(message)s',
								filename=logFile,
								filemode='a')

		# Notification method
		self.notifyMethod = notifyMethod
	
		# To group messages by status type
		self.msgPass   = ''
		self.msgWarn   = ''
		self.msgFail   = ''
		self.countPass = 0
		self.countWarn = 0
		self.countFail = 0

	# Print message and save it as a fail, warn or pass type
	def log(self, type, message, echo = False):

		if echo: print(message)

		if type == 'fail':
			self.countFail += 1
			self.msgFail   += message + '\n'
			if 'log' in self.notifyMethod: logging.error(message)
		elif type == 'warn':
			self.countWarn += 1
			self.msgWarn   += message + '\n'
			if 'log' in self.notifyMethod: logging.warn(message)
		elif type == 'pass':
			self.countPass += 1
			self.msgPass   += message + '\n'
			if 'log' in self.notifyMethod: logging.info(message)
		else:
			if 'log' in self.notifyMethod: logging.info(message)

	# Report, or send, result message
	# Any messages are cleared
	def report(self, stage, echo = False, header = False):
		returnCode = True
		statusMsg  = 'Successful'
		statusCode = 'SUCCESS'
		
		if header:
			message    = f'{header}\n\n'
		else:
			message    = ''

		# Collect messages and figure out result
		if self.msgFail:
			message   += 'Failed\n' + self.msgFail + '\n'
			statusMsg  = 'Had failures'
			statusCode = 'FAILED'

		if self.msgWarn:
			if self.msgFail:
				message += '\n'
			else:
				statusMsg  = 'Had warnings'
				statusCode = 'WARNING'
			message += 'Warnings\n' + self.msgWarn + '\n'

		if self.msgPass:
			if self.msgWarn or self.msgFail: message += '\nSuccessful\n'
			message += self.msgPass + '\n'

		# Status code is also dependent on stage
		if stage == 'start':
			statusCode = 'STARTED_' + statusCode
		elif stage == 'running':
			if statusCode == 'SUCCESS':
				statusCode = 'RUNNING'
			elif statusCode == 'FAILED':
				statusCode = 'RUNNING_ERROR'
			else:
				statusCode = 'RUNNING_' + statusCode
		elif stage == 'stopped':
			statusCode = 'FAILED'
			statusMsg  = 'Failed'
			returnCode = False
		elif stage == 'complete':
			if statusCode == 'FAILED':
				statusCode = 'COMPLETED_' + statusCode
			else:
				statusCode = 'COMPLETED_' + statusCode

		# Print and report status result as specified
		if 'monitor' in self.notifyMethod: notifyJM(self.jobCode, statusCode, message)
		if echo:
			print(statusMsg)
			print(message)

		# Clear messages
		self.msgPass   = ''
		self.msgWarn   = ''
		self.msgFail   = ''
		self.countPass = 0
		self.countWarn = 0
		self.countFail = 0

		return returnCode

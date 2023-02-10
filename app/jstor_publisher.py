import sys, os, os.path, json, requests, traceback, time, boto3
from tenacity import retry, retry_if_result, wait_random_exponential, retry_if_not_exception_type
from datetime import datetime
from flask import Flask, request, jsonify, current_app, make_response
from random import randint
from time import sleep
from pymongo import MongoClient
import fnmatch

harvest_ignore_dirs = (os.environ.get('HARVEST_IGNORE_DIRS','')).split(',')
transform_ignore_dirs = (os.environ.get('TRANSFORM_IGNORE_DIRS','')).split(',')
ignore_dirs = harvest_ignore_dirs + transform_ignore_dirs

class JstorPublisher():
    def __init__(self):
        self.child_running_jobs = []
        self.child_error_jobs = []
        self.child_success_jobs = []
        self.parent_job_ticket_id = None
        self.child_error_count = 0
        self.max_child_errors = int(os.getenv("CHILD_ERROR_LIMIT", 10))
        self.via_access_key = os.environ.get('S3_VIA_ACCESS_KEY')
        self.via_secret_key = os.environ.get('S3_VIA_SECRET_KEY')
        self.via_bucket_name = os.environ.get('S3_VIA_BUCKET')
        self.via_s3_endpoint = os.environ.get('S3_VIA_ENDPOINT')
        self.via_s3_region = os.environ.get('S3_VIA_REGION')
        self.via_s3_resource = None
        self.via_s3_bucket = None
        self.ssio_access_key = os.environ.get('S3_SSIO_ACCESS_KEY')
        self.ssio_secret_key = os.environ.get('S3_SSIO_SECRET_KEY')
        self.ssio_bucket_name = os.environ.get('S3_SSIO_BUCKET')
        self.ssio_s3_endpoint = os.environ.get('S3_SSIO_ENDPOINT')
        self.ssio_s3_region = os.environ.get('S3_SSIO_REGION')
        self.ssio_s3_resource = None
        self.ssio_s3_bucket = None
        self.aspace_access_key = os.environ.get('S3_ASPACE_ACCESS_KEY')
        self.aspace_secret_key = os.environ.get('S3_ASPACE_SECRET_KEY')
        self.aspace_bucket_name = os.environ.get('S3_ASPACE_BUCKET')
        self.aspace_s3_endpoint = os.environ.get('S3_ASPACE_ENDPOINT')
        self.aspace_s3_region = os.environ.get('S3_ASPACE_REGION')
        self.aspace_s3_resource = None
        self.aspace_s3_bucket = None
        self.connect_to_buckets()
        
    def connect_to_buckets(self):
        """
            Connect to the via, ssio, & aspace s3 buckets
        """
        via_boto_session = boto3.Session(aws_access_key_id=self.via_access_key, aws_secret_access_key=self.via_secret_key)
        self.via_s3_resource = via_boto_session.resource('s3')
        self.via_s3_bucket = self.via_s3_resource.Bucket(self.via_bucket_name)

        ssio_boto_session = boto3.Session(aws_access_key_id=self.ssio_access_key, aws_secret_access_key=self.ssio_secret_key)
        self.ssio_s3_resource = ssio_boto_session.resource('s3')
        self.ssio_s3_bucket = self.ssio_s3_resource.Bucket(self.ssio_bucket_name)

        aspace_boto_session = boto3.Session(aws_access_key_id=self.aspace_access_key, aws_secret_access_key=self.aspace_secret_key)
        self.aspace_s3_resource = aspace_boto_session.resource('s3')
        self.aspace_s3_bucket = self.aspace_s3_resource.Bucket(self.aspace_bucket_name)

    # Write to error log update result and update job tracker file
    def handle_errors(self, result, error_msg, exception_msg = None, set_job_failed = False):
        exception_msg = str(exception_msg)
        current_app.logger.error(exception_msg)
        current_app.logger.error(error_msg)
        result['error'] = error_msg
        result['message'] = exception_msg
        # Append error to job tracker file errors_encountered list
        if self.parent_job_ticket_id:
            job_tracker.append_error(self.parent_job_ticket_id, error_msg, exception_msg, set_job_failed)

        return result

    def do_task(self, request_json):
        """\
Get job tracker file
Append job ticket id to jobs in process list in the tracker file
Update job timestamp file"""

        result = {
          'success': False,
          'error': None,
          'message': ''
        }

        #Get the job ticket which should be the parent ticket
        current_app.logger.error("**************JStor Publisher: Do Task**************")
        current_app.logger.error("WORKER NUMBER " + str(os.getenv('CONTAINER_NUMBER')))

        result['success'] = True
        # altered line so we can see request json coming through properly
        result['message'] = 'Job ticket id {} has completed '.format(request_json['job_ticket_id'])

        sleep_s = int(os.getenv("TASK_SLEEP_S", 1))

        current_app.logger.info("Sleep " + str(sleep_s) + "seconds")
        sleep(sleep_s)

        jstorforum = False
        if 'jstorforum' in request_json:
            current_app.logger.info("running jstorforum harvest")
            jstorforum = request_json['jstorforum']
        if jstorforum:
            self.do_publish()
        
        aspace = False
        if 'aspace' in request_json:
            current_app.logger.info("running aspace transform")
            aspace = request_json['aspace']
        if aspace:
            self.do_publish('aspace')

        #dump json
        current_app.logger.info("json message: " + json.dumps(request_json))

        #integration test: write small record to mongo to prove connectivity
        integration_test = False
        if ('integration_test' in request_json):
            integration_test = request_json['integration_test']
        if (integration_test):
            current_app.logger.info("running integration test")
            try:
                self.do_publish(True)
            except Exception as err:
                current_app.logger.error("Error: unable to publish records, {}", err)
            try:
                mongo_url = os.environ.get('MONGO_URL')
                mongo_dbname = os.environ.get('MONGO_DBNAME')
                mongo_collection = os.environ.get('MONGO_COLLECTION_ITEST')
                mongo_client = MongoClient(mongo_url, maxPoolSize=1)

                mongo_db = mongo_client[mongo_dbname]
                integration_collection = mongo_db[mongo_collection]
                job_ticket_id = str(request_json['job_ticket_id'])
                test_id = "publisher-" + job_ticket_id
                test_record = { "id": test_id, "status": "inserted" }
                integration_collection.insert_one(test_record)
                mongo_client.close()
            except Exception as err:
                current_app.logger.error("Error: unable to connect to mongodb, {}", err)
        
        return result

    def do_publish(self, itest=False):
        if itest:
            configfile = os.getenv("JSTOR_HARVEST_TEST_CONFIG")
        else:
            configfile = os.getenv("JSTOR_HARVEST_CONFIG")
        current_app.logger.info("configfile: " + configfile)
        with open(configfile) as f:
            harvjobsjson = f.read()
        harvestconfig = json.loads(harvjobsjson)
        harvestDir = os.getenv("JSTOR_HARVEST_DIR")        
        transformDir = os.getenv("JSTOR_TRANSFORM_DIR")
        aspaceDir = os.getenv("JSTOR_ASPACE_DIR")
        directories = [harvestDir, transformDir]
        #publish to VIA and SSIO
        current_app.logger.info("publishing to S3")
        for baseDir in directories:
            for job in harvestconfig:     
                if job["jobName"] == "jstorforum":   
                    for set in job["harvests"]["sets"]:
                        setSpec = "{}".format(set["setSpec"])
                        opDir = set["opDir"]
                        currentPath = baseDir + "/" + opDir
                        current_app.logger.info("looking in current path: " + currentPath)
                        if os.path.exists(currentPath):
                            if len(fnmatch.filter(os.listdir(currentPath), '*.xml')) > 0:
                                current_app.logger.info("Publishing set: " + opDir)
                                for filename in os.listdir(currentPath):
                                    try:
                                        filepath = currentPath + "/" + filename
                                        s3prefix = opDir + "/"
                                        if (baseDir == harvestDir):  #send to SSIO bucket
                                            current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the SSIO bucket") 
                                            self.ssio_s3_bucket.upload_file(filepath, s3prefix + filename)
                                        elif (baseDir == transformDir):  #send to VIA bucket
                                            current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the VIA bucket") 
                                            self.via_s3_bucket.upload_file(filepath, s3prefix + filename)
                                    except Exception as err:
                                        current_app.logger.error("VIA/SSIO Publishing error: {}", err)
                #publish to Aspace
                if job["jobName"] == "aspace":                      
                    if os.path.exists(aspaceDir):
                        if len(fnmatch.filter(os.listdir(aspaceDir), '*.xml')) > 0:
                            current_app.logger.info("Publishing to Aspace S3")
                            for filename in os.listdir(aspaceDir):
                                try:
                                    filepath = aspaceDir + "/" + filename
                                    current_app.logger.info("Uploading: " + filepath + " to " + filename + " in the ASPACE bucket")
                                    self.aspace_s3_bucket.upload_file(filepath, filename)
                                except Exception as err:
                                    current_app.logger.error("Aspace Publishing error: {}", err)

    def revert_task(self, job_ticket_id, task_name):
        return True

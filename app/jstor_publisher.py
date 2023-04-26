import sys, os, os.path, json, requests, traceback, time, boto3, subprocess, re
from tenacity import retry, retry_if_result, wait_random_exponential, retry_if_not_exception_type
from datetime import datetime
from flask import Flask, request, jsonify, current_app, make_response
from random import randint
from time import sleep
from pymongo import MongoClient
import fnmatch
from ast import literal_eval

harvest_ignore_dirs = (os.environ.get('HARVEST_IGNORE_DIRS','')).split(',')
transform_ignore_dirs = (os.environ.get('TRANSFORM_IGNORE_DIRS','')).split(',')
ignore_dirs = harvest_ignore_dirs + transform_ignore_dirs
concat_script_path= os.environ.get('CONCAT_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/concat-files.sh')
publish_lc_incr_script_path= os.environ.get('PUBLISH_LC_INCR_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-lc-incr.sh')
publish_lc_full_script_path= os.environ.get('PUBLISH_LC_FULL_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-lc-full.sh')
publish_lc_full_set_script_path= os.environ.get('PUBLISH_LC_FULL_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-lc-full-set.sh')
publish_primo_incr_script_path= os.environ.get('PUBLISH_PRIMO_INCR_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-primo-incr.sh')
publish_primo_full_script_path= os.environ.get('PUBLISH_PRIMO_FULL_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-primo-full.sh')
publish_primo_full_set_script_path= os.environ.get('PUBLISH_PRIMO_FULL_SET_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/publish-primo-full-set.sh')
via_script_path = os.environ.get('VIA_SCRIPT_PATH','/home/jstorforumadmltstools/via/bin/via_export.py')
weed_script_path = os.environ.get('WEED_SCRIPT_PATH','/home/jstorforumadm/ltstools/bin/weed_files.py')
try:
    weed_files_flag = literal_eval(os.environ.get('WEED_FILES', 'False'))
except ValueError:
    weed_files_flag = False
try:
    publish_to_primo = literal_eval(os.environ.get('PUBLISH_PRIMO', 'False'))
except ValueError:
    publish_to_primo = False
try:
    publish_to_lc = literal_eval(os.environ.get('PUBLISH_LC', 'False'))
except ValueError:
    publish_to_lc = False

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
        self.repositories = self.load_repositories()

        
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

        #dump json
        current_app.logger.info("json message: " + json.dumps(request_json))

        job_ticket_id = str(request_json['job_ticket_id'])

        #integration test: write small record to mongo to prove connectivity
        integration_test = False
        if ('integration_test' in request_json):
            integration_test = request_json['integration_test']

        harvestset = None
        if 'harvestset' in request_json:
            harvestset = request_json["harvestset"]
        
        until_field = None
        if 'until' in request_json:
            until_field = request_json["until"].replace("-", "")
        
        harvest_date = None
        if 'harvestdate' in request_json:
            harvest_date = request_json["harvestdate"].replace("-", "")

        jstorforum = False
        if 'jstorforum' in request_json:
            current_app.logger.info("running jstorforum publisher")
            jstorforum = request_json['jstorforum']
        if jstorforum:
            try:
                self.do_publish('jstorforum', harvestset, job_ticket_id, False, harvest_date, until_field)
            except Exception as err:
                current_app.logger.error("Error: unable to publish jstorforum records", exc_info=True)

        aspace = False
        if 'aspace' in request_json:
            current_app.logger.info("running aspace transform")
            aspace = request_json['aspace']
        if aspace:
            try:
                self.do_publish('aspace', None, job_ticket_id, False, harvest_date, until_field)
            except Exception as err:
                current_app.logger.error("Error: unable to publish aspace records", exc_info=True)

        #integration test: write small record to mongo to prove connectivity
        integration_test = False
        if ('integration_test' in request_json):
            integration_test = request_json['integration_test']
        if (integration_test):
            current_app.logger.info("running integration test")

            try:
                self.do_publish('jstorforum', harvestset, job_ticket_id, True, harvest_date, until_field)
            except Exception as err:
                current_app.logger.error("Error: unable to publish jstorforum records in itest", exc_info=True)

            try:
                self.do_publish('aspace', None, job_ticket_id, True, harvest_date, until_field)
            except Exception as err:
                current_app.logger.error("Error: unable to publish aspace records in itest", exc_info=True)
            
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
                current_app.logger.error("Error: unable to connect to mongodb", exc_info=True)

        #call weed files script
        if (weed_files_flag):
            if (self.weed_files()):
                current_app.logger.info("weeding files successful")
            else:
                current_app.logger.error("weeding files failed")
        
        return result

    def do_publish(self, jobname, harvestset, job_ticket_id, itest=False, harvest_date=None, until_field=None):
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
        dropsDir = os.getenv("JSTOR_DROPS_DIR")
        deletesDir = os.getenv("JSTOR_DELETES_DIR")
        aspaceDir = os.getenv("JSTOR_ASPACE_DIR")
        directories = [harvestDir, transformDir]
        mongo_url = os.environ.get('MONGO_URL')
        mongo_dbname = os.environ.get('MONGO_DBNAME')
        harvest_collection_name = os.environ.get('HARVEST_COLLECTION', 'jstor_published_summary')
        repository_collection_name = os.environ.get('REPOSITORY_COLLECTION', 'jstor_repositories')
        record_collection_name = os.environ.get('JSTOR_PUBLISHED_RECORDS', 'jstor_published_records')
        mongo_url = os.environ.get('MONGO_URL')
        mongo_client = None
        mongo_db = None
        try:
            mongo_client = MongoClient(mongo_url, maxPoolSize=1)
            mongo_db = mongo_client[mongo_dbname]
        except Exception as err:
            current_app.logger.error("Error: unable to connect to mongodb", exc_info=True)
        
        deletedIds = []
        primoIds = []
        lcIds = []
        reRecordId = re.compile('.+deleteRecordId.*\>(\w+\d+)\<\/deleteRecordId.+')
        reRecordId2 = re.compile('.+recordId.*\>(\w+\d+)\<\/recordId.+')

        #publish to VIA and SSIO
        current_app.logger.info("Publishing to S3")
        for baseDir in directories:
            for job in harvestconfig:     
                if jobname == 'jstorforum' and jobname == job["jobName"]:    
                    for set in job["harvests"]["sets"]:
                        publish_successful = True 
                        setSpec = "{}".format(set["setSpec"])
                        repository_name = self.repositories[setSpec]["displayname"]
                        repo_short_name = self.repositories[setSpec]["shortname"]
                        opDir = set["opDir"]
                        currentPath = baseDir + "/" + opDir
                        hollisTransformedPath = baseDir + "/" + opDir + "_hollis"
                        totalPublishCount = 0
                        harvestdate = datetime.today().strftime('%Y-%m-%d') 
                        if harvestset is None:
                            current_app.logger.info("looking in current path: " + currentPath)
                            if os.path.exists(currentPath):
                                if len(fnmatch.filter(os.listdir(currentPath), '*.xml')) > 0:
                                    current_app.logger.info("Publishing set: " + opDir)
                                    for filename in os.listdir(currentPath):
                                        identifier = filename[:-4]
                                        destination = "VIA/SSIO"
                                        try:
                                            filepath = currentPath + "/" + filename
                                            s3prefix = opDir + "/"
                                            if (baseDir == harvestDir):  #send to SSIO bucket
                                                current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the SSIO bucket") 
                                                self.ssio_s3_bucket.upload_file(filepath, s3prefix + filename)
                                                destination = "SSIO"
                                            elif (baseDir == transformDir):  #send to VIA bucket
                                                current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the VIA bucket") 
                                                self.via_s3_bucket.upload_file(filepath, s3prefix + filename)
                                                destination = "VIA"
                                            if (baseDir == transformDir):  #add this id to the list of files that will go to librarycloud
                                                lcRecord = {"job_ticket_id": job_ticket_id, "identifier": identifier, "status": "add_update", 
                                                        "harvestdate": harvestdate, "setSpec": setSpec, "repository_name": repository_name, "repo_short_name": repo_short_name}
                                                lcIds.append(lcRecord)
                                                totalPublishCount = totalPublishCount + 1 
                                            #write/update record
                                            try:
                                                status = "add_update"
                                                success = True
                                                self.write_record(job_ticket_id, identifier, harvestdate, setSpec, repository_name, repo_short_name, 
                                                    status, record_collection_name, success, destination, mongo_db)   
                                            except Exception as e:
                                                current_app.logger.error("Mongo error writing " + setSpec + " record: " +  identifier, exc_info=True)
                                        except Exception as err:
                                            current_app.logger.error("VIA/SSIO Publishing error", exc_info=True)
                                            publish_successful = False
                                            try:
                                                status = "add_update"
                                                success = False
                                                self.write_record(job_ticket_id, identifier, harvestdate, setSpec, repository_name, repo_short_name, 
                                                    status, record_collection_name, success, destination, mongo_db, err)    
                                            except Exception as e:
                                                current_app.logger.error("Mongo error writing " + setSpec + " record: " +  identifier, exc_info=True)

                            if (os.path.exists(hollisTransformedPath) and (baseDir == transformDir)): #gather list of ids that will go to hollis (primo)
                                current_app.logger.info("looking for ids to be published "+
                                    "to primo in current path: " + hollisTransformedPath)
                                if len(fnmatch.filter(os.listdir(hollisTransformedPath), '*.xml')) > 0:
                                    for filename in os.listdir(hollisTransformedPath):
                                        identifier = filename[:-4]
                                        primoRecord = {"job_ticket_id": job_ticket_id, "identifier": identifier, "status": "add_update", 
                                                "harvestdate": harvestdate, "setSpec": setSpec, "repository_name": repository_name, "repo_short_name": repo_short_name}
                                        primoIds.append(primoRecord)

                        elif  setSpec == harvestset: 
                            current_app.logger.info("Publishing for only one set: " + setSpec)
                            current_app.logger.info("looking in current path: " + currentPath)
                            if os.path.exists(currentPath):
                                if len(fnmatch.filter(os.listdir(currentPath), '*.xml')) > 0:
                                    current_app.logger.info("Publishing set: " + opDir)
                                    for filename in os.listdir(currentPath):
                                        identifier = filename[:-4]
                                        destination = "VIA/SSIO"
                                        try:
                                            filepath = currentPath + "/" + filename
                                            s3prefix = opDir + "/"
                                            if (baseDir == harvestDir):  #send to SSIO bucket
                                                current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the SSIO bucket") 
                                                self.ssio_s3_bucket.upload_file(filepath, s3prefix + filename)
                                                destination = "SSIO"
                                            elif (baseDir == transformDir):  #send to VIA bucket
                                                current_app.logger.info("Uploading: " + filepath + " to " + s3prefix + filename + " in the VIA bucket") 
                                                self.via_s3_bucket.upload_file(filepath, s3prefix + filename)
                                                destination = "VIA"
                                            if (baseDir == transformDir):  #add this id to the list of files that will go to librarycloud 
                                                lcRecord = {"job_ticket_id": job_ticket_id, "identifier": identifier, "status": "add_update", 
                                                    "harvestdate": harvestdate, "setSpec": setSpec, "repository_name": repository_name, "repo_short_name": repo_short_name}
                                                lcIds.append(lcRecord)
                                                totalPublishCount = totalPublishCount + 1
                                            #write/update record
                                            try:
                                                status = "add_update"
                                                success = True
                                                self.write_record(job_ticket_id, identifier, harvestdate, setSpec, repository_name, repo_short_name, 
                                                    status, record_collection_name, success, destination, mongo_db)  
                                            except Exception as e:
                                                current_app.logger.error("Mongo error writing " + setSpec + " record: " +  identifier, exc_info=True)
                                        except Exception as err:
                                            current_app.logger.error("VIA/SSIO Publishing error", exc_info=True)
                                            publish_successful = False
                                            #log error to mongo
                                            try:
                                                status = "add_update"
                                                success = False
                                                self.write_record(job_ticket_id, identifier, harvestdate, setSpec, repository_name, repo_short_name, 
                                                    status, record_collection_name, success, destination, mongo_db, err)   
                                            except Exception as e:
                                                current_app.logger.error("Mongo error writing " + setSpec + " record: " +  identifier, exc_info=True)     

                            if (os.path.exists(hollisTransformedPath) and (baseDir == transformDir)): #gather list of ids that will go to hollis (primo)
                                current_app.logger.info("looking for ids to be published "+
                                    "to primo in current path: " + hollisTransformedPath)
                                if len(fnmatch.filter(os.listdir(hollisTransformedPath), '*.xml')) > 0:
                                    for filename in os.listdir(hollisTransformedPath):
                                        identifier = filename[:-4]
                                        primoRecord = {"job_ticket_id": job_ticket_id, "identifier": identifier, "status": "add_update",  
                                                "harvestdate": harvestdate, "setSpec": setSpec, "repository_name": repository_name, "repo_short_name": repo_short_name}
                                        primoIds.append(primoRecord)

                        #update harvest record
                        try:
                            if (baseDir == transformDir):
                                self.write_harvest(job_ticket_id, harvestdate, setSpec, 
                                    repository_name, repo_short_name, totalPublishCount, harvest_collection_name, mongo_db, jobname, publish_successful)
                        except Exception as e:
                            current_app.logger.error("Mongo error writing harvest record for : " +  setSpec, exc_info=True) 

        #publish to Aspace
        #if jobname == 'aspace' and jobname == job["jobName"]:  
        if  ((jobname == 'aspace') and (any('aspace' in job["jobName"] for job in harvestconfig))):                     
            harvestdate = datetime.today().strftime('%Y-%m-%d')
            totalPublishCount = 0
            publish_successful = True
            if os.path.exists(aspaceDir):
                if len(fnmatch.filter(os.listdir(aspaceDir), '*.xml')) > 0:
                    current_app.logger.info("Publishing to Aspace S3")
                    destination = "Aspace"
                    for filename in os.listdir(aspaceDir):
                        identifier = filename[:-4]
                        try:
                            filepath = aspaceDir + "/" + filename
                            current_app.logger.info("Uploading: " + filepath + " to " + filename + " in the ASPACE bucket")
                            self.aspace_s3_bucket.upload_file(filepath, filename)
                            totalPublishCount = totalPublishCount + 1
                            #write/update record
                            try:
                                status = "add_update"
                                success = True
                                self.write_record(job_ticket_id, identifier, harvestdate, "0000", "aspace", "ASP", 
                                    status, record_collection_name, success, destination, mongo_db)   
                            except Exception as e:
                                current_app.logger.error("Mongo error writing aspace record: " +  identifier, exc_info=True)
                        except Exception as err:
                            current_app.logger.error("Aspace Publishing error", exc_info=True)
                            publish_successful = False
                            #log error to mongo
                            try:
                                status = "add_update"
                                success = False
                                self.write_record(job_ticket_id, identifier, harvestdate, "0000", "aspace", "ASP", 
                                    status, record_collection_name, success, destination, mongo_db, err)  
                            except Exception as e:
                                current_app.logger.error("Mongo error writing aspace record " +  identifier, exc_info=True)
            #update harvest record
            try:
                self.write_harvest(job_ticket_id, harvestdate, "0000", 
                    "aspace", "ASP", totalPublishCount, harvest_collection_name, mongo_db, jobname, publish_successful)
            except Exception as e:
                current_app.logger.error("Mongo error writing harvest record for: aspace", exc_info=True)

        if (jobname == 'jstorforum'):
            #mark deleted records
            current_app.logger.info("Mark deleted records")
            if os.path.exists(deletesDir):
                harvestdate = datetime.today().strftime('%Y-%m-%d')
                status = "delete"
                success = True
                if len(fnmatch.filter(os.listdir(deletesDir), '*.xml')) > 0:
                    for filename in os.listdir(deletesDir):
                        if (filename.endswith(".xml") and not filename.startswith("via_export_del_")):
                            try:
                                setspec, identifier = (filename[:-4]).split("_", 1)
                            except:
                                continue
                            repository_name = self.repositories[setspec]["displayname"]
                            repo_short_name = self.repositories[setspec]["shortname"]
                            try:
                                self.write_record(job_ticket_id, identifier, harvestdate, setspec, repository_name, repo_short_name, 
                                    status, record_collection_name, success, "lc", mongo_db) 
                                self.write_record(job_ticket_id, identifier, harvestdate, setspec, repository_name, repo_short_name, 
                                    status, record_collection_name, success, "primo", mongo_db) 
                            except Exception as e:
                                current_app.logger.error("Mongo error writing deleted records", exc_info=True)
            lcPublishSuccess = False
            primoPublishSuccess = False
            #TODO - get full run flag 
            concatFileSuccess = self.concat_files(harvestset, harvest_date, until_field)

            if (concatFileSuccess):
                #call via export incremental script for Primo (Hollis Inages)
                if (publish_to_primo):
                    current_app.logger.info("Publishing to Primo...")
                    primoPublishSuccess = self.export_files("incr", "primo")
                    if (primoPublishSuccess):
                        current_app.logger.info("Publishing to Primo successful")
                    else:
                        current_app.logger.info("Publishing to Primo failed")
                else:
                    current_app.logger.info("Publish to Primo skipped")
                    #call via export incremental script for Librarycloud
                if (publish_to_lc):
                    current_app.logger.info("Publishing to Librarycloud...")
                    lcPublishSuccess = self.export_files("incr", "lc")
                    if (lcPublishSuccess):
                        current_app.logger.info("Publishing to Librarycloud successful")
                    else:
                        current_app.logger.info("Publishing to Librarycloud failed")
                else:
                    current_app.logger.info("Publish to Librarycloud skipped")
            else:
                if (not publish_to_lc):
                    current_app.logger.info("Publish to LC skipped")
                if (not publish_to_primo):
                    current_app.logger.info("Publish to Primo skipped")

            #update mongo with librarycloud and primo record lists
            for primoRec in primoIds:
                try:
                    error = None
                    if (not primoPublishSuccess):
                        error = "export failed"
                    if (not publish_to_primo):
                        error = "not exported"
                    self.write_record(job_ticket_id, primoRec["identifier"], primoRec["harvestdate"], 
                        primoRec["setSpec"], primoRec["repository_name"], primoRec["repo_short_name"], primoRec["status"], 
                        record_collection_name, primoPublishSuccess, "primo", mongo_db, error)  
                except Exception as e:
                    current_app.logger.error("Mongo error writing primo record: " +  primoRec["identifier"], exc_info=True)

            for lcRec in lcIds:
                try:
                    error = None
                    if (not lcPublishSuccess):
                        error = "export failed"
                    if (not publish_to_lc):
                        error = "not exported"
                    self.write_record(job_ticket_id, lcRec["identifier"], lcRec["harvestdate"], 
                        lcRec["setSpec"], lcRec["repository_name"], lcRec["repo_short_name"], lcRec["status"], 
                        record_collection_name, primoPublishSuccess, "lc", mongo_db, error)  
                except Exception as e:
                    current_app.logger.error("Mongo error writing primo record: " +  primoRec["identifier"], exc_info=True)

        if (mongo_client is not None):            
            mongo_client.close()
                
    
    def write_record(self, harvest_id, record_id, harvest_date, repository_id, repository_name, repo_short_name, 
            status, collection_name, success, destination, mongo_db, error=None):
        if mongo_db == None:
            current_app.logger.info("Error: mongo db not instantiated")
            return
        try:
            err_msg = ""
            if error != None:
                    err_msg = error
            if harvest_date == None: #set harvest date to today if harvest date is None
                harvest_date = datetime.today().strftime('%Y-%m-%d')  
            harvest_date_obj = datetime.strptime(harvest_date, "%Y-%m-%d")
            last_update = datetime.now()
            harvest_record = { "harvest_id": harvest_id, "last_update": last_update, "harvest_date": harvest_date_obj, "record_id": record_id, 
                "repository_id": repository_id, "repository_name": repository_name, "repo_short_name": repo_short_name, 
                "status": status, "success": success, "destination": destination, "error": err_msg } 
            record_collection = mongo_db[collection_name]
            record_collection.insert_one(harvest_record)
            #record_collection.update_one(query, harvest_record, upsert=True)
            current_app.logger.info("record " + str(record_id) + " of repo " + str(repository_id) + " written to mongo ")
        except Exception as err:
            current_app.logger.info("Error: unable to connect to mongodb", exc_info=True)
        return
    
    def write_harvest(self, harvest_id, harvest_date, repository_id, repository_name, repo_short_name, 
            total_published, collection_name, mongo_db, jobname, success):
        if mongo_db == None:
            current_app.logger.info("Error: mongo db not instantiated")
            return
        try:
            if harvest_date == None: #set harvest date to today if harvest date is None
                harvest_date = datetime.today().strftime('%Y-%m-%d') 
            harvest_date_obj = datetime.strptime(harvest_date, "%Y-%m-%d")
            last_update = datetime.now()
            harvest_record = { "id": harvest_id, "last_update": last_update, "harvest_date": harvest_date_obj, 
                "repository_id": repository_id, "repository_name": repository_name, "repo_short_name": repo_short_name, 
                "total_published_count": total_published, "success": success, "jobname": jobname }
            harvest_collection = mongo_db[collection_name]
            harvest_collection.insert_one(harvest_record)
            current_app.logger.info(repository_name + " harvest for " + harvest_date + " written to mongo ")
        except Exception as err:
            current_app.logger.info("Error: unable to connect to mongodb", exc_info=True)
        return

    def load_repositories(self):
        repositories = {}
        try:
            mongo_url = os.environ.get('MONGO_URL')
            mongo_dbname = os.environ.get('MONGO_DBNAME')
            repository_collection_name = os.environ.get('REPOSITORY_COLLECTION', 'jstor_repositories')
            mongo_url = os.environ.get('MONGO_URL')
            mongo_dbname = os.environ.get('MONGO_DBNAME')
            mongo_client = MongoClient(mongo_url, maxPoolSize=1)

            mongo_db = mongo_client[mongo_dbname]
            repository_collection = mongo_db[repository_collection_name]
            repos = repository_collection.find({})
            for r in repos:
                k = r["_id"]
                v = { "displayname": r["displayname"], "shortname": r["shortname"] }
                repositories[k] = v 
            mongo_client.close()
            return repositories
        except Exception as err:
            current_app.logger.info("Error: unable to load repository table from mongodb", exc_info=True)
            return repositories

    def concat_files(self, harvestset = None, harvestdate = None, until_field = None, fullrun= None):
        #concatenate files for primo and librarycloud
        concat_opts = ""
        if (harvestset != None):
            concat_opts = concat_opts + " -s " + harvestset
        if (harvestdate != None): 
            concat_opts = concat_opts + " -d " + harvestdate
        if (until_field != None):
            concat_opts = concat_opts + " -u " + until_field
        if (fullrun != None):
            concat_opts = concat_opts + " -l " + fullrun
        try:
            subprocess.check_call([concat_script_path + concat_opts], shell=True)
            current_app.logger.info("LC and Primo file concatenation successful")
            return True
        except Exception as e:
            current_app.logger.error("File concatenation failed: Primo and LC publish aborted", exc_info=True)
            return False

    def export_files(self, size, dest):
        #call via export incremental script for Primo (Hollis Inages)
        try:
            if (dest == "lc"):
                if (size == "incr"):
                    subprocess.check_call([publish_lc_incr_script_path])
                elif (size == "full"):
                    if (harvestset != None):
                        subprocess.check_call([publish_lc_full_set_script_path + " " + harvestset])
                    else:
                        subprocess.check_call([publish_lc_full_script_path])
            elif (dest == "primo"):
                if (size == "incr"):
                    subprocess.check_call([publish_primo_incr_script_path])
                elif (size == "full"):
                    if (harvestset != None):
                        subprocess.check_call([publish_primo_full_set_script_path + " " + harvestset])
                    else:
                        subprocess.check_call([publish_primo_full_script_path])
            return True
        except Exception as e:
            current_app.logger.error(dest + " " + size + " export script error", exc_info=True)
            return False
    
    def weed_files(self):
        #call weed files script
        try:
            subprocess.check_call([weed_script_path])
            return True
        except Exception as e:
            current_app.logger.error("Delete script error", exc_info=True)
            return False

    #add more sophisticated healthchecking later
    def healthcheck(self):
        hc = "OK"
        return hc

    def revert_task(self, job_ticket_id, task_name):
        return True

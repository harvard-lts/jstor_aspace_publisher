#!groovy
@Library('lts-basic-pipeline') _

// projName is the directory name for the project on the servers for it's docker/config files
// default values: 
//  registryCredentialsId = "${env.REGISTRY_ID}"
//  registryUri = 'https://registry.lts.harvard.edu'
def endpoints = []
ltsBasicPipeline.call("jstor-aspace-publisher", "JSTOR", "jstor", "", endpoints, "lts-jstorforum-alerts")

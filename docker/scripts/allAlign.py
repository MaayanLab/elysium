#!/usr/bin/python
import datetime, time
import subprocess
import shlex
import os
import os.path
import sys
import tinys3
import glob
import urllib2
import urllib
import boto
from boto.s3.key import Key
import requests
import json

awsid = os.environ['AWSID']
awskey = os.environ['AWSKEY']
cloudpass = os.environ['CLOUDPASS']

def uploadS3(file, key, bucket):
    conn = tinys3.Connection(awsid, awskey, tls=True)
    f = open(file,'rb')
    conn.upload(key, f, bucket)

def basename(p):
    temp = p.split("/")
    return temp[len(temp)-1]

r = requests.get("https://amp.pharm.mssm.edu/cloudalignment/givejob?pass="+cloudpass)
jj = r.json()

if jj['id'] != "empty":
    if str(jj['type']) == "sequencing":
        links = jj['datalinks'].split(";")
        
        for ll in links:
            ll = str(ll)
            fb = basename(ll)
            
            conn = boto.connect_s3(awsid, awskey)
            bucket = conn.get_bucket(jj['resultbucket'])
            key = bucket.get_key(jj['userid']+'/'+fb)
            key.get_contents_to_filename("/alignment/data/uploads/"+fb)
        
        filenames = next(os.walk("/alignment/data/uploads"))[2]
        organism = str(jj['organism'])
        index = "/alignment/data/index/"+organism+"_index.idx"
        indexlink = "https://s3.amazonaws.com/mssm-seq-index/"+organism+"_index.idx"
        
        # load index if not already loaded
        if not os.path.isfile(index):
            print("Load index file")
            urllib.urlretrieve(indexlink, index)
        
        if len(filenames) == 1:
            with open("/alignment/data/results/runinfo.txt", "w") as f:
                subprocess.call(shlex.split("/alignment/tools/kallisto/stu"+index+" --single -l 200 -s 20 -o /alignment/data/results /alignment/data/uploads/"+filenames[0]), stderr=f)
        
        if len(filenames) == 2:
            with open("/alignment/data/results/runinfo.txt", "w") as f:
                subprocess.call(shlex.split("/alignment/tools/kallisto/kallisto quant -t 2 -i "+index+" -o /alignment/data/results /alignment/data/uploads/"+filenames[0]+" /alignment/data/uploads/"+filenames[1]), stderr=f)
        
        print("Kallisto quantification completed")
        
        uploadS3("/alignment/data/results/abundance.tsv", str(jj['userid'])+"/"+str(jj['outname'])+"_transcript.tsv", "biodos")
        
        print("Uploaded raw counts to S3")
        
        mapping = "/alignment/data/mapping/"+organism+"_mapping.rda"
        
        if not os.path.isfile(mapping):
            print("Load gene mapping information")
            mappinglink = "https://s3.amazonaws.com/mssm-seq-genemapping/"+organism+"_mapping.rda"
            urllib.urlretrieve(mappinglink, mapping)
        
        subprocess.call(shlex.split("Rscript --vanilla scripts/genelevel.r "+mapping))
        
        uploadS3("/alignment/data/results/runinfo.txt", str(jj['userid'])+"/"+str(jj['outname'])+"_qc.tsv", "biodos")
        uploadS3("/alignment/data/results/gene_abundance.tsv", str(jj['userid'])+"/"+str(jj['outname'])+"_gene.tsv", "biodos")
        
        #send kallisto quantification to database
        r = requests.get('https://amp.pharm.mssm.edu/cloudalignment/finishjob?pass='+cloudpass+'&uid='+str(jj['uid']))
        print("Sent counts to database")
        # clean up after yourself, a bit risky if python fails files fill up container space
        files = glob.glob('/alignment/data/results/*')
        for f in files:
            os.remove(f)
        
        files = glob.glob('/alignment/data/uploads/*')
        for f in files:
            os.remove(f)

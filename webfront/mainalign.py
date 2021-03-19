import datetime
import tornado.escape
import tornado.ioloop
import tornado.web
import os
import io
import string
import random
import requests
import pymysql
import hashlib
import base64
import hmac
import uuid
import boto3
import json
import time
import threading
import sys
import math
import urllib3


root = os.path.dirname(__file__)

dbhost = os.environ['DBHOST']
dbuser = os.environ['DBUSER']
dbpasswd = os.environ['DBPASSWD']
dbname = os.environ['DBNAME']
bucketname = os.environ['BUCKET']
charon_url = os.environ['CHARON']

jobpasswd = os.environ['JOBPASSWD']

scalefactor = int(os.environ['INSTANCESCALE'])
mininstances = int(os.environ['MININSTANCES'])
maxinstances = int(os.environ['MAXINSTANCES'])

awsid = os.environ['AWSID']
awskey = os.environ['AWSKEY']
autoscaling_name = os.environ['AUTOSCALINGGROUP']

minQueueSize = 20
maxQueueSize = 100
jobQueueARCHS4 = []

lockUpdate = False

def getConnection():
    return pymysql.connect(host=dbhost, user=dbuser, password=dbpasswd, database=dbname)

def getInstanceCount():
    client = boto3.client('autoscaling',
                          aws_access_key_id=awsid,
                          aws_secret_access_key=awskey,
                          region_name="us-east-1"
                          )
    response = client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[
            autoscaling_name,
        ]
    )
    res = response["AutoScalingGroups"][0]
    return len(res['Instances'])

def scaleGroup(size):
    client = boto3.client('autoscaling',
        aws_access_key_id=awsid,
        aws_secret_access_key=awskey,
        region_name="us-east-1"
    )
    response = client.update_auto_scaling_group(
        AutoScalingGroupName=autoscaling_name,
        MinSize=size,
        MaxSize=size,
        DesiredCapacity=size
    )

def refillJobQueueARCHS4():
    
    db = getConnection()
    cur = db.cursor()
    query = "SELECT id, uid, resultbucket, datalinks, parameters FROM sequencing WHERE status='waiting' LIMIT 100"
    cur.execute(query)
    
    list_of_ids = []
    
    for res in cur:
        list_of_ids.append(res[0])
        response = {}
        response["id"] = res[0]
        response["uid"] = res[1]
        response["type"] = "sequencing"
        response["resultbucket"] = res[2]
        response["datalinks"] = res[3]
        response["parameters"] = res[4]
        jobQueueARCHS4.append(response)
    
    format_strings = ','.join(['%s'] * len(list_of_ids))
    
    cur.execute("UPDATE sequencing SET status = 'submitted', datesubmitted=now() WHERE id IN (%s)" % format_strings, tuple(list_of_ids));
    
    db.commit()
    cur.close()
    db.close()

def ec2thread(mininstances, maxinstances, scalefactor):
    while True:
        try:
            
            time.sleep(300)
            db = getConnection()
            cur = db.cursor()
            query = "SELECT id FROM jobqueue WHERE status='waiting' LIMIT 100"
            cur.execute(query)
            cur.close()
            db.close()
            counter = 0
            
            for res in cur:
                counter = counter+1
            
            current_instance_count = getInstanceCount()
            
            if counter > 0 and current_instance_count == 0:
                scaleGroup(maxinstances)
            elif counter == 0 and current_instance_count > 0:
                time.sleep(600)
                scaleGroup(mininstances)
        except:
            print("Loop had an issue")

class AlignmentProgressHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        username = self.get_argument('username', True)
        password = self.get_argument('password', True)
        prefix = self.get_argument('prefix', True)
        pstatus = self.get_argument('status', True)
        
        print("in the get request function")
        print(username)
        print(password)
        print(prefix)
        
        url = charon_url+"/login?username="+username+"&password="+password
        http = urllib3.PoolManager()
        response = http.request('GET', url).data
        data = json.loads(response)

        if data["status"] == "success":
            uuid = data["message"]
            db = getConnection()
            cur = db.cursor()
            
            if pstatus == True:
                if prefix != True:
                    query = "SELECT * FROM jobqueue WHERE userid=%s AND outname LIKE CONCAT(%s, '%%') ORDER BY id ASC"
                    cur.execute(query, (uuid, prefix, ))
                else:
                    query = "SELECT * FROM jobqueue WHERE userid=%s ORDER BY id ASC"
                    cur.execute(query, (uuid, ))
            elif prefix != True:
                query = "SELECT * FROM jobqueue WHERE userid=%s AND status=%s AND outname LIKE CONCAT(%s, '%%') ORDER BY id ASC"
                cur.execute(query, (uuid, pstatus, prefix, ))
            else:
                query = "SELECT * FROM jobqueue WHERE userid=%s AND status=%s ORDER BY id ASC"
                cur.execute(query, (uuid, pstatus, ))
            
            jobs = {}
            for res in cur:
                status = res[6]
                salt = res[5]
                fname = res[3]
                
                job = { 'id': res[0],
                         'uid': res[1],
                         'user': res[2],
                         'datalink': res[3],
                         'outname': res[4],
                         'organism': res[5],
                         'status': res[6],
                         'creationdate': str(res[7]),
                         'submissiondate': str(res[8]),
                         'finishdate': str(res[9])}
                jobs[res[0]] = job
            cur.close()
            db.close()
            self.write(jobs)
        else:
            response = { 'action': 'list jobs',
                 'task': username,
                 'status': 'error',
                 'message': 'login failed'}
            self.write(response)

class CreateJobHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        username = self.get_argument('username', True)
        password = self.get_argument('password', True)
        file1 = self.get_argument('file1', True)
        file2 = self.get_argument('file2', True)
        organism = self.get_argument('organism', True)
        outname = self.get_argument('outname', True)
        
        url = charon_url+"/login?username="+username+"&password="+password
        http = urllib3.PoolManager()
        response = http.request('GET', url).data
        data = json.loads(response)
        uuid = data["message"]
        
        if data["status"] == "success":
            
            print(file1)
            print(file2)
            
            if file2 == True:
                datalink = "https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file1
                
                h2 = hashlib.md5()
                h2.update((outname+datalink).encode('utf-8'))
                uid = h2.hexdigest()
                
                db = getConnection()
                cur = db.cursor()
                query = "INSERT INTO jobqueue (uid, userid, datalink, outname, organism) VALUES (%s, %s, %s, %s, %s)"
                
                cur.execute(query, (uid, uuid, datalink, outname, organism, ))
                db.commit()
                cur.close()
                db.close()
                response = { 'action': 'create job',
                     'task': username,
                     'status': 'success',
                     'message': uid}
                self.write(response)
            else:
                datalink = "https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file1+";"+"https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file2
                
                h2 = hashlib.md5()
                h2.update((outname+datalink).encode('utf-8'))
                uid = h2.hexdigest()
                
                db = getConnection()
                cur = db.cursor()
                query = "INSERT INTO jobqueue (uid, userid, datalink, outname, organism) VALUES (%s, %s, %s, %s, %s)"
                
                cur.execute(query, (uid, uuid, datalink, outname, organism, ))
                db.commit()
                cur.close()
                db.close()
                response = { 'action': 'create job',
                     'task': username,
                     'status': 'success',
                     'message': uid}
                self.write(response)


class QueueViewHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        username = self.get_argument('username', True)
        password = self.get_argument('password', True)
        
        url = charon_url+"/login?username="+str(username)+"&password="+str(password)
        http = urllib3.PoolManager()
        response = http.request('GET', url).data
        data = json.loads(response)
        uuid = data["message"]
        
        if data["status"] == "success":
            db = getConnection()
            cur = db.cursor()
            
            query = "SELECT * FROM jobqueue WHERE status='waiting' OR status='submitted'"
            
            cur.execute(query)
            
            response = {}
            
            subm = []
            usr = []
            datalinks = []
            outnames = []
            species = []
            
            for res in cur:
                if res[2] == data["message"]:
                    usr.append(1)
                    datalinks.append(res[3])
                    outnames.append(res[4])
                    species.append(res[5])
                else:
                    usr.append(0)
                    datalinks.append("")
                    outnames.append("")
                    species.append("")
                
                if res[6] == "submitted":
                    subm.append(2)
                else:
                    subm.append(1)
            
            response = { 'user': usr,
                         'submissionstatus': subm,
                         'files': datalinks,
                         'outname': outnames,
                         'organism': species,
                         'status': 'success'}
            self.write(response)
        else:
            response = { 'status': 'failed',
                         'message': 'credential error'}
            self.write(response)

class VersionHandler(tornado.web.RequestHandler):
    def get(self):
        response = { 'version': '1',
                     'last_build':  datetime.date.today().isoformat() }
        self.write(response)

class GiveJobHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        jpass = self.get_argument('pass', True)
        response = {}
        response["id"] = "empty"
        
        if jpass == jobpasswd:
            db = getConnection()
            cur = db.cursor()
            query = "SELECT * FROM jobqueue WHERE status='waiting' LIMIT 1"
            cur.execute(query)
            
            for res in cur:
                response["id"] = res[0]
                response["uid"] = res[1]
                response["userid"] = res[2]
                response["type"] = "sequencing"
                response["resultbucket"] = bucketname
                response["datalinks"] = res[3]
                response["outname"] = res[4]
                response["organism"] = res[5]
                query = "UPDATE jobqueue SET status='submitted', submissiondate=now() WHERE id=%s"
                cur.execute(query, (res[0], ))
                db.commit()
            cur.close()
            db.close()
        self.write(response)

class GiveJobHandlerArchs4Old(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        
        response = {}
        response["id"] = "empty"
        
        db = getConnection()
        cur = db.cursor()
        query = "SELECT id, uid, resultbucket, datalinks, parameters FROM sequencing WHERE status='waiting' LIMIT 1"
        cur.execute(query)
        
        for res in cur:
            print("handing out job")
            print(res)
            response["id"] = res[0]
            response["uid"] = res[1]
            response["type"] = "sequencing"
            response["resultbucket"] = res[2]
            response["datalinks"] = res[3]
            response["parameters"] = res[4]
            query = "UPDATE sequencing SET status='submitted', datesubmitted=now() WHERE id=%s"
            cur.execute(query, (res[0], ))
            db.commit()
        cur.close()
        db.close()
        self.write(response)

class GiveJobHandlerArchs4(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        print("Queue Size: "+str(len(jobQueueARCHS4)))
        if len(jobQueueARCHS4) < minQueueSize:
            #lockUpdate = True
            refillJobQueueARCHS4()
            #lockUpdate = False
        
        response = {}
        
        if len(jobQueueARCHS4) == 0:
            response["id"] = "empty"
        else:
            response = jobQueueARCHS4.pop(0)
        print(response)
        self.write(response)

class FinishJobHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        jpass = self.get_argument('pass', True)
        uid = self.get_argument('uid', True)
        
        response = {}
        if jpass == jobpasswd:
            db = getConnection()
            cur = db.cursor()
            query = "UPDATE jobqueue SET status='completed', finishdate=now() WHERE uid=%s"
            cur.execute(query, (uid,))
            db.commit()
            cur.close()
            db.close()
            response["id"] = uid
            response["status"] = "completed"
        else:
            response["id"] = uid
            response["status"] = "failed"
        self.write(response)

class FinishJobHandlerArchs4(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        data = json.loads(self.request.body)
        
        jpass = data["pass"]
        uid = data["uid"]
        listid = int(data["id"])
        nreads = int(data["nreads"])
        naligned = int(data["naligned"])
        nlength = int(data["nlength"])
        
        print(data)
        
        response = {}
        if jpass == jobpasswd:
            print("get connection")
            db = getConnection()
            cur = db.cursor()
            query = "UPDATE sequencing SET status='completed', datecompleted=now() WHERE uid=%s"
            cur.execute(query, (uid,))
            db.commit()
            print("updated sequencing")
            
            query = "INSERT INTO runinfo (listid, nreads, naligned, nlength) VALUES (%s, %s, %s, %s)"
            cur.execute(query, (listid, nreads, naligned, nlength,))
            db.commit()
            cur.close()
            db.close()
            
            print("inserted runinfo")
            
            response["id"] = uid
            response["status"] = "completed"
        else:
            response["id"] = uid
            response["status"] = "credentials failed"
        self.write(response)

application = tornado.web.Application([
    (r"/cloudalignment/version", VersionHandler),
    (r"/cloudalignment/givejob", GiveJobHandler),
    (r"/cloudalignment/givejobarchs4", GiveJobHandlerArchs4),
    (r"/cloudalignment/finishjob", FinishJobHandler),
    (r"/cloudalignment/finishjobarchs4", FinishJobHandlerArchs4),
    (r"/cloudalignment/createjob", CreateJobHandler),
    (r"/cloudalignment/progress", AlignmentProgressHandler),
    (r"/cloudalignment/queueview", QueueViewHandler),
    (r"/cloudalignment/(.*)", tornado.web.StaticFileHandler, dict(path=root))
])

ec2t = threading.Thread(target=ec2thread, args=(mininstances, maxinstances, scalefactor, ))
ec2t.start()

if __name__ == "__main__":
    application.listen(5000)
    tornado.ioloop.IOLoop.instance().start()

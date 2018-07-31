import datetime
import tornado.escape
import tornado.ioloop
import tornado.web
import os
import io
import string
import random
import requests
import MySQLdb
import hashlib
import base64
import hmac
import uuid
import boto
import json
import urllib
import time
import threading
import math

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


def getConnection():
    db = MySQLdb.connect(host=dbhost,  # your host, usually localhost
                     user=dbuser,      # your username
                     passwd=dbpasswd,  # your password
                     db=dbname)        # name of the data base
    return(db)

def ec2thread(mininstances, maxinstances, scalefactor):
    while True:
        db = getConnection()
        cur = db.cursor()
        cur2 = db.cursor()
        
        # get current minsize (assuming equal to active instances)
        asg = os.popen("/root/.local/bin/aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name EC2ContainerService-cloudalignment-EcsInstanceAsg-HL25MFFL1T8Q").read()
        jasg = json.loads(asg)
        currentInstances = jasg['AutoScalingGroups'][0]['MinSize']
        
        query = "SELECT id, submissiondate FROM jobqueue WHERE status='submitted'"
        cur.execute(query)
        for res in cur:
            print((datetime.datetime.utcnow() - res[1]).total_seconds()/60)
            if (datetime.datetime.utcnow() - res[1]).total_seconds()/60 > 60:
                query = "UPDATE jobqueue SET status='failed' WHERE id='"+str(res[0])+"'"
                cur2.execute(query)
        
        db.commit()
        
        cur = db.cursor()
        query = "SELECT COUNT(*) FROM jobqueue WHERE status='submitted' OR status='waiting'"
        cur.execute(query)
        count = cur.fetchone()[0]
        
        # there is no jobs in the queue that are currently processing or waiting, scale number of nodes to mininstances
        if count == 0 and int(currentInstances) > int(mininstances):
            os.system("/root/.local/bin/aws autoscaling update-auto-scaling-group --auto-scaling-group-name EC2ContainerService-cloudalignment-EcsInstanceAsg-HL25MFFL1T8Q --min-size "+str(mininstances)+" --max-size "+str(mininstances)+" --desired-capacity "+str(mininstances))
        
        elif count > 0:
            cur = db.cursor()
            query = "SELECT COUNT(*) FROM jobqueue WHERE status='waiting'"
            cur.execute(query)
            count = cur.fetchone()[0]
            
            instanceCount = str(max(mininstances, min(maxinstances, int(math.ceil(count/float(scalefactor))))))
            
            if int(instanceCount) > int(currentInstances):
                print("scale up to "+str(instanceCount)+" from "+str(currentInstances))
                os.system("/root/.local/bin/aws autoscaling update-auto-scaling-group --auto-scaling-group-name EC2ContainerService-cloudalignment-EcsInstanceAsg-HL25MFFL1T8Q --min-size "+instanceCount+" --max-size "+instanceCount+" --desired-capacity "+instanceCount)
        
        db.close()
        time.sleep(30)

class AlignmentProgressHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        username = self.get_argument('username', True)
        password = self.get_argument('password', True)
        pstatus = self.get_argument('status', True)
        
        url = charon_url+"/login?username="+username+"&password="+password
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        uuid = data["message"]
        
        if data["status"] == "success":
            db = getConnection()
            cur = db.cursor()
            
            if pstatus == True:
                query = "SELECT * FROM jobqueue WHERE userid='%s' ORDER BY id ASC" % (uuid)
            else:
                query = "SELECT * FROM jobqueue WHERE userid='%s' AND status='%s' ORDER BY id ASC" % (uuid, pstatus)
            
            cur.execute(query)
            
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
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        uuid = data["message"]
        
        if data["status"] == "success":
            url = charon_url+"/files?username="+username+"&password="+password
            response = urllib.urlopen(url)
            data = json.loads(response.read())
            
            files = data["filenames"]
            print(file1)
            print(file2)
            
            if file2 == True:
                if (file1 in files):
                    datalink = "https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file1
                    
                    h2 = hashlib.md5()
                    h2.update((datalink).encode('utf-8'))
                    uid = h2.hexdigest()
                    
                    db = getConnection()
                    cur = db.cursor()
                    query = "INSERT INTO jobqueue (uid, userid, datalink, outname, organism) VALUES ('%s', '%s', '%s', '%s', '%s')" % (uid, uuid, datalink, outname, organism)
                    
                    cur.execute(query)
                    db.commit()
                    cur.close()
                    db.close()
                    response = { 'action': 'create job',
                         'task': username,
                         'status': 'success',
                         'message': uid}
                    self.write(response)
                else:
                    response = { 'action': 'create job',
                         'task': username,
                         'status': 'error',
                         'message': 'file not found'}
                    self.write(response)
            else:
                if (file1 in files) & (file2 in files):
                    datalink = "https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file1+";"+"https://s3.amazonaws.com/"+bucketname+"/"+uuid+"/"+file2
                    
                    h2 = hashlib.md5()
                    h2.update((datalink).encode('utf-8'))
                    uid = h2.hexdigest()
                    
                    db = getConnection()
                    cur = db.cursor()
                    query = "INSERT INTO jobqueue (uid, userid, datalink, outname, organism) VALUES ('%s', '%s', '%s', '%s', '%s')" % (uid, uuid, datalink, outname, organism)
                    
                    cur.execute(query)
                    db.commit()
                    cur.close()
                    db.close()
                    response = { 'action': 'create job',
                         'task': username,
                         'status': 'success',
                         'message': uid}
                    self.write(response)
                else:
                    response = { 'action': 'create job',
                         'task': username,
                         'status': 'error',
                         'message': 'files not found'}
                    self.write(response)

class QueueViewHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        username = self.get_argument('username', True)
        password = self.get_argument('password', True)
        
        url = charon_url+"/login?username="+username+"&password="+password
        response = urllib.urlopen(url)
        data = json.loads(response.read())
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
                query = "UPDATE jobqueue SET status='submitted', submissiondate=now() WHERE id='%s'" % (res[0])
                cur.execute(query)
                db.commit()
        
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
            query = "UPDATE jobqueue SET status='completed', finishdate=now() WHERE uid='%s'" % (uid)
            cur.execute(query)
            db.commit()
            
            response["id"] = uid
            response["status"] = "completed"
        else:
            response["id"] = uid
            response["status"] = "failed"
        
        self.write(response)

application = tornado.web.Application([
    (r"/cloudalignment/version", VersionHandler),
    (r"/cloudalignment/givejob", GiveJobHandler),
    (r"/cloudalignment/finishjob", FinishJobHandler),
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






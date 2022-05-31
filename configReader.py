import sys
import traceback
from configparser import ConfigParser
import datetime
import logging
# from Application.GetLogs import getLogFile
import os
try:
    import cst
except:
    pass
import base64
import json
#######################################################################################################################


loc1 = os.getcwd().split('Application')


config_location ='config_json.json'


#######################################################################################################################
def readConfig_All():
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        mdtoken = jConfig['MDToken']['token']
        iToken = jConfig['IAToken']['token']
        userid = jConfig['userID']
        apiip = jConfig['url']
        source = jConfig['Credentials']['source']
        market_data_appKey = jConfig['Credentials'] ['marketdata_appkey']
        market_data_secretKey =jConfig['Credentials'] ['marketdata_secretkey']
        ia_appKey = jConfig['Credentials'] ['interactive_appkey']
        ia_secretKey = jConfig['Credentials'] ['interactive_secretkey']
        clist = jConfig['IAToken'] ['Client_list']
        iheaders = {
            'authorization': iToken,
            'Content-Type': 'application/json'
        }
        mheaders = {
            'authorization': mdtoken,
            'Content-Type': 'application/json'
        }
        # loginid = jConfig['MDToken'] ['loginid']
        DClient = jConfig['DefaultClient']
        broadcastMode = jConfig ['broadcastmode']
        f1.close()
        return (mheaders,iheaders,mdtoken,iToken,apiip,userid,source,market_data_appKey,market_data_secretKey,ia_appKey,ia_secretKey,clist,DClient,broadcastMode)
    except:
        print(config_location)
        logging.error(sys.exc_info()[1])
#####################################################################################
x=readConfig_All()
print(x)
#####################################################################################


def writeITR(token,userID,client_list):
    try:
        # print('IAS Token',token)
        f1 = open(config_location)
        jConfig = json.load(f1)
        f1.close()
        now = datetime.datetime.now()
        current_date = now.strftime("%y-%m-%d")
        current_time = now.strftime("%H:%M:%S")

        jConfig['IAToken'] ['token']= token
        jConfig ['userID']= userID
        jConfig['IAToken'] ['login_date']= current_date
        jConfig['IAToken'] ['login_time']= current_time
        jConfig['IAToken'] ['Client_list']= client_list

        jConfig_new = json.dumps(jConfig,indent=4)

        f2 = open(config_location,'w')
        f2.write(jConfig_new)
        f2.close()
    except:
        print(traceback.print_exc())
        logging.error(sys.exc_info()[1])



def writeMD(token,userID):
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        f1.close()
        now = datetime.datetime.now()
        current_date = now.strftime("%y-%m-%d")
        current_time = now.strftime("%H:%M:%S")

        jConfig['MDToken']['token'] =token
        jConfig['userID'] =userID
        jConfig['MDToken']['login_time']= current_time

        jConfig_new = json.dumps(jConfig,indent=4)

        f2 = open(config_location,'w')
        f2.write(jConfig_new)
    except:
        print(traceback.print_exc())
        logging.error(sys.exc_info()[1])




def writeURL(a):
    try:
        if(a=='ARHAM_DIRECT'):
            url = cst.ZD
        elif(a=='ARHAM_WEB'):
            url = cst.ZW
        elif (a =='TRADECIRCLE_DIRECT' ):
            url = cst.SD
        elif (a == 'TRADECIRCLE_WEB'):
            url = cst.SW
        elif (a == 'UAT'):
            url = cst.XD
        elif (a == 'UAT_WEB'):
            url = cst.XW
        url1 = base64.urlsafe_b64decode(url.encode("utf-8")).decode('utf-8')
        f1 = open(config_location)
        jConfig = json.load(f1)
        jConfig[ 'url']= url1

        f1.close()



        f2 = open(config_location)
        f2.write(jConfig)
    except:
        logging.error(sys.exc_info()[1])



def updateConfig(MDKey = '',MDSecret = '',IAKey = '',IASecret = '',LoginID = ''):
    config = ConfigParser()
    config.read(config_location)

    ######################################################################################
    if(MDKey != ''):
        config.set('Credentials', 'marketdata_appkey', MDKey)
    elif(MDSecret != ''):
        config.set('Credentials', 'marketdata_secretkey', MDSecret)
    elif(IAKey != ''):
        config.set('Credentials', 'interactive_appkey', IAKey)
    elif(IASecret != ''):
        config.set('Credentials', 'interactive_secretkey', IASecret)
    elif(LoginID != ''):
        config.set('MDToken', 'loginid', LoginID)
    ########################################################################################

    with open(config_location, 'w') as f:
        config.write(f)



def readLoginId():
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        loginid = jConfig['MDToken'] ['loginid']
        f1.close()
        return loginid
    except:
        logging.error(sys.exc_info()[1])


def readClist():
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        clist = jConfig['IAToken'] ['Client_list']
        f1.close()
        return clist
    except:
        logging.error(sys.exc_info()[1])

def readBroadcastMode():
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        broadcastMode = jConfig ['broadcastmode']

        f1.close()

        return broadcastMode
    except:
        logging.error(sys.exc_info()[1])


def readDefaultClient():
    try:
        f1 = open(config_location)
        jConfig = json.load(f1)
        DClient = jConfig['DefaultClient']

        f1.close()

        return DClient
    except:
        logging.error(sys.exc_info()[1])


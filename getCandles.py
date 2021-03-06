from Masters import *
from configReader import *

from os import makedirs,path

# login
def login():
    try:
        mheaders, iheaders, mToken, iToken, apiip, userid, source, market_data_appKey, market_data_secretKey, ia_appKey, ia_secretKey, clist, DClient, broadcastMode = readConfig_All()
        refresh_config()
        payload = {"secretKey": self.IASecret,"appKey": self.IAKey,"source": self.source}
        login_url = self.URL + '/interactive/user/session'
        login_access = requests.post(login_url, json=payload)
        # print(login_url,login_access.text)
        logging.info(str(login_access.text).replace('\n','\t\t\t\t'))

        data = login_access.json()
        if login_access.status_code == 200:
            data = login_access.json()
            result = data['result']
            a='successfull'
            token = result['token']
            #####################################  clist  ###########################################################
            writeITR(token,self.userID,self.client_list)
    except:
        print(traceback.print_exc())
        logging.error(sys.exc_info())

######################
def opnfdbtb2(self):
    try:
        today = datetime.datetime.today().strftime('%d%m%Y')
        abc = 'candle backup\\' + today
        if not path.exists(abc):
            makedirs(abc)

        # self.fname = filename[0]
        # self.lineEdit.setText(self.fname)
    except:
        print(sys.exc_info())



def backup_write(token,mheaders):
    segment = 1
    apiip = 'http://192.168.102.9:3000'
    compression_val = 1
    c = datetime.datetime.today().strftime('%b %d %Y 091500')
    # print(token)
    b = datetime.datetime.today().strftime('%b %d %Y 153000')

    url = '%s/marketdata/instruments/ohlc?exchangeSegment=%s&exchangeInstrumentID=%s&startTime=%s&endTime=%s&compressionValue=%s' % (
        apiip, segment,
        token, c, b, compression_val)
    a = requests.get(url, headers=mheaders)

    b = (a.json()['result']['dataReponse'])
    # print(b)
    c = b.split(',')
    return c

login()
a=readConfig_All()

mheaders, iheaders, mToken, iToken, apiip, userid, source, market_data_appKey, market_data_secretKey, ia_appKey, ia_secretKey, clist, DClient, broadcastMode = readConfig_All()


contract_full,contract_fo,contract_eq,contract_cd,heads = get_contract_master(False)

today = datetime.datetime.today().strftime('%d%m%Y')
filename = 'BackupData//%s.csv'%today
print(filename)
f = open(filename, "w")
for i in contract_eq:
    # print(i)
    token = i[2]
    c = backup_write(token,mheaders)

    for k in c:
        d = k.split('|')
        if k == '':
            pass
        else:
            bttt = datetime.datetime.utcfromtimestamp(int(d[0])).strftime('%b %d %H:%M:%S')
            # print(str(i[2]) + '|' + bttt + '|' + k + '\n')
            f.write('%s|%s|%s|%s|%s\n'%(i[2],i[3],i[6],bttt, k ))
f.close()

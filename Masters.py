import time
import traceback

import numpy as np
import pandas as pd
import datatable as dt
import requests
import sys
import json
import numpy
import os
from configReader import *
# configReaderloc1 = os.getcwd().split('Application')
contractDir = 'contract_df.csv'
# print()

def segwork( a):
    if (a == 3.0 or a == 4.0):
        aa= 'O'
    else:
        aa= 'F'

    return aa


def otwork( a):
    if (a == 3):
        return 'CE'
    elif (a == 4):
        return 'PE'
    else:
        return ' '


def spwork( a):
    if(a == ''):
        return ' '
    if(a == 0):
        return ' '
    elif (isinstance(a, float) or isinstance(a, int)):
        return '%.2f' % a


def expwork(a):
    try:
        if isinstance(a, int) or isinstance(a, float):
            aa = ' '
        else:
            aa = a.replace('-', '')[0:8]
    except:
        print(sys.exc_info(),'exp a: ',type(a))
    return aa


def assetTokenWork1( a,b):
    if(a==-1):
        if(b=='Nifty 50'):
            return '26000'
        elif(b=='Nifty Bank'):
            return  '26001'
        elif (b == 'Nifty Fin Service'):
            return '26002'

    else:

        a = str(a)
        aa = a.replace('110010000', '')
        aaa = aa.replace('11001000', '')
        return aaa



def strkwork1( z):
    if (z == '0.00'):
        x = ' '
    if (z == 0):
        x = ' '
    else:
        x = '%.2f' % z
    return x

def expWork1( z):
    try:
        x = datetime.datetime.strptime(z, '%d-%b-%Y').strftime('%Y%m%d')
        return x
    except:
        print(traceback.print_exc())
        return ' '



import os
import requests
import zipfile
import datetime

def get_bhavcopy():
    user_agent = 'Mozilla/5.0'
    headers = {'User-Agent': user_agent}
    try:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        fo_bhav1 = "https://archives.nseindia.com/content/historical/DERIVATIVES/" + yesterday.strftime(
            '%Y') + r"/" + yesterday.strftime('%b').upper() + r"/fo" + yesterday.strftime(
            "%d%b%Y").upper() + "bhav.csv.zip"
        cm_bhav = "https://archives.nseindia.com/content/historical/EQUITIES/" + yesterday.strftime(
            '%Y') + r"/" + yesterday.strftime(
            '%b').upper() + r"/cm" + yesterday.strftime("%d%b%Y").upper() + r"bhav.csv.zip"
        ind_bhav = r"https://archives.nseindia.com/content/indices/ind_close_all_" + yesterday.strftime(
            '%d%m%Y') + ".csv"
        fo_bhav1 = "https://www1.nseindia.com/content/historical/DERIVATIVES/" + yesterday.strftime(
            '%Y') + r"/" + yesterday.strftime('%b').upper() + r"/fo" + yesterday.strftime(
            "%d%b%Y").upper() + "bhav.csv.zip"
        cm_bhav = "https://www1.nseindia.com/content/historical/EQUITIES/" + yesterday.strftime(
            '%Y') + r"/" + yesterday.strftime(
            '%b').upper() + r"/cm" + yesterday.strftime("%d%b%Y").upper() + r"bhav.csv.zip"
        ind_bhav = r"https://www1.nseindia.com/content/indices/ind_close_all_" + yesterday.strftime(
            '%d%m%Y') + ".csv"

        # https://www1.nseindia.com/content/indices/ind_close_all_06092021.csv
        dat = yesterday.strftime('%d%b%Y').upper()
        dt = yesterday

        re = requests.get(fo_bhav1, headers={'User-Agent': user_agent})

        with open("fo_bhav.zip", 'wb') as f:
            f.write(re.content)
        f.close()

        zf = zipfile.ZipFile('fo_bhav.zip', 'r')
        zf.extractall()
        zf.close()

        if os.path.isfile("fo_bhav.csv"):
            os.remove("fo_bhav.csv")
        fname = 'fo' + dat + 'bhav.csv'

        sf = fname

        os.rename(sf, 'fo_bhav.csv')

        req = requests.get(cm_bhav, headers={'User-Agent': user_agent})
        with open("cm_bhav.zip", 'wb') as f:
            f.write(req.content)
        f.close()

        zf = zipfile.ZipFile('cm_bhav.zip', 'r')
        zf.extractall()
        zf.close()
        if os.path.isfile("cm_bhav.csv"):
            os.remove("cm_bhav.csv")
        fname = 'cm' + dat + 'bhav.csv'

        sf = fname

        os.rename(sf, 'cm_bhav.csv')
        a = dt.strftime('%d%m%Y')

        rez = requests.get(ind_bhav, headers={'User-Agent': user_agent})

        with open("idx_bhav.csv", 'wb') as f:
            f.write(rez.content)

    except:
        print(traceback.print_exc())


def assetTokenWork( a):

    a = str(a)
    # print(a,b)


    aa = a.replace('110010000', '')
    aaa = aa.replace('11001000', '')
    return aaa







def get_contract_master(validation):
    try:
        Symbol_Expiry_Dict = {}
        if(validation==True):
            get_bhavcopy()
            mheaders, iheaders, mToken, iToken, apiip, userid, source,market_data_appKey,market_data_secretKey,ia_appKey,ia_secretKey,clist,DClient,broadcastMode = readConfig_All()
            sub_url = apiip + '/marketdata/instruments/master'

            ###################################### NSE FNO #################################

            payloadsub = {"exchangeSegmentList": ["NSEFO"]}
            payloadsubjson = json.dumps(payloadsub)
            req = requests.request("POST", sub_url, data=payloadsubjson, headers=mheaders)
            data_p = req.json()
            abc = data_p['result']
            ####################################################################################

            ############################# Save as Raw Text ###############################
            contractFO_raw1 = os.path.join(loc1[0] ,'Resourses','contractFO_raw.txt')
            with open (contractFO_raw1,'w') as f:
                f.write(abc)
            f.close()
            ####################################################################################


            contractFo1 = pd.read_csv(contractFO_raw1, header=None, sep='|'
                                      ,names=['ExchangeSegment', 'Token', 'instrument_type1', 'symbol', 'Stock_name',
                                                'instrument_type', ' NameWithSeries', 'InstrumentID', 'PriceBand.High',
                                                'PriceBand.Low','FreezeQty', 'tick_size', 'lot_size', 'Multiplier',
                                                 'UnderlyingInstrumentId','IndexName','ContractExpiration', 'strike1', 'OptionType'])

            contractFo1 = contractFo1[contractFo1['instrument_type1']!=4] # type 4 is spread contract that we should include but on later stage of product dev

            contractFo1['Exchange'] = 'NSEFO'
            # contractFo1['Segment'] = 'F'
            contractFo1['Segment'] = contractFo1['OptionType'].apply(segwork)
            contractFo1['option_type'] = contractFo1['OptionType'].apply(otwork)
            contractFo1['strike1'] = contractFo1['strike1'].fillna(0)
            contractFo1['strike1'] = contractFo1['strike1'].astype('float')
            contractFo1['strike_price'] = contractFo1['strike1'].apply(spwork)
            contractFo1['exp'] = contractFo1['ContractExpiration'].apply(expwork)
            contractFo1['asset_token'] = contractFo1[['UnderlyingInstrumentId','IndexName']].apply(lambda x: assetTokenWork1(x.UnderlyingInstrumentId, x.IndexName), axis=1)
            contractFo1['FreezeQty'] = contractFo1['FreezeQty'] - 1

            cndf1 = contractFo1[['Exchange',
                                 'Segment','Token', 'symbol', 'Stock_name', 'instrument_type',
                                 'exp', 'strike_price', 'option_type','asset_token', 'tick_size',
                                 'lot_size', 'strike1','Multiplier','FreezeQty','PriceBand.High',
                                 'PriceBand.Low']]

            cndfo = dt.Frame(cndf1)
            d = cndf1.to_numpy()
            contract_fo = cndf1.to_numpy()

            contract_fo = (contract_fo[contract_fo[:, 2].argsort()])
            strat_point = (contract_fo[0][2])
            end_point = (contract_fo[-1][2])
            gap = end_point - strat_point
            temp_df1 = np.arange(start=35000, stop=gap + 1, step=1)
            raw_token = contract_fo[:, 2]
            total_token = np.hstack([raw_token, temp_df1])
            v, r = np.unique(total_token, return_counts=True)
            unique_token = v[np.where(r == 1)]

            # print(unique_token,unique_token.shape)
            # print('unique_token.shape',unique_token.shape)

            temp_rows = np.empty((unique_token.shape[0], 17), dtype=object)
            temp_rows[:, 2] = unique_token
            temp_rows[:, [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,15,16]] = ['NSEFO',
                                                                                   '', '', '', '', '',
                                                                                   '', '', 0.0,0.0, 0,
                                                                                   0.0, 0.0, 0,0.0,0.0]
            contract_fo = np.vstack([contract_fo, temp_rows])
            contract_fo = (contract_fo[contract_fo[:, 2].argsort()])

        ##################### adding extra columns for additional fields ##########
            bdf = np.zeros((contract_fo.shape[0], 21))
            contract_fo = np.hstack([contract_fo, bdf])
        ################################################################

        ########################  Expiry Type #########################
            unique_symbols = np.unique(contract_fo[:, 3])

            for i in unique_symbols:
                # time.sleep(2)
                Symbol_Expiry_Dict[i] = ['', '', '', '', '', '', '']
                fltr = np.asarray([i])
                filteredDf = contract_fo[np.in1d(contract_fo[:, 3], fltr)]

                fltr1 = np.asarray(['OPTIDX'])
                filteredDf1 = filteredDf[np.in1d(filteredDf[:, 5], fltr1)]
                unique_exp = np.unique(filteredDf[:, 6])
                unique_exp = unique_exp[unique_exp.argsort()]

                for jk, ik in enumerate(unique_exp):

                    fltr1 = np.asarray([ik])
                    token_list1 = filteredDf1[np.in1d(filteredDf1[:, 6], fltr1), 2]
                    token_list = np.subtract(token_list1, 35000).tolist()

                    if (jk == 0):
                        Symbol_Expiry_Dict[i][0] = ik
                        contract_fo[token_list, 33] = 1
                    elif (jk == 1):
                        Symbol_Expiry_Dict[i][1] = ik
                        contract_fo[token_list, 33] = 2
                    else:
                        Symbol_Expiry_Dict[i][2] = ik
                        contract_fo[token_list, 33] = 3
                fltr1 = np.asarray(['FUTSTK', 'FUTIDX'])
                filteredDf1 = filteredDf[np.in1d(filteredDf[:, 5], fltr1)]

                unique_exp = np.unique(filteredDf1[:, 6])
                unique_exp = unique_exp[unique_exp.argsort()]

                for jk, ik in enumerate(unique_exp):
                    fltr1 = np.asarray([ik])
                    token_list1 = filteredDf[np.in1d(filteredDf[:, 6], fltr1), 2]
                    token_list = np.subtract(token_list1, 35000).tolist()

                    if (jk == 0):
                        if (ik == Symbol_Expiry_Dict[i][0]):
                            Symbol_Expiry_Dict[i][3] = ik
                            contract_fo[token_list, 33] = 4
                        else:
                            Symbol_Expiry_Dict[i][4] = ik
                            contract_fo[token_list, 33] = 5
                    elif (jk == 1):
                        Symbol_Expiry_Dict[i][5] = ik
                        contract_fo[token_list, 33] = 6
                    else:
                        Symbol_Expiry_Dict[i][6] = ik
                        contract_fo[token_list, 33] = 7

        ################################################################


        ###################### working for strike diff ####################
            unique_symbols = np.unique(contract_fo[:, 3])
            for i in unique_symbols:
                if (i != ''):
                    fltr = np.asarray([i])
                    filteredDf = contract_fo[np.in1d(contract_fo[:, 3], fltr)]
                    fltr1 = np.asarray(['OPTSTK', 'OPTIDX'])
                    filteredDf1 = filteredDf[np.in1d(filteredDf[:, 5], fltr1)]

                    uniquq_strk = np.unique(filteredDf1[:, 7]).astype('float')
                    uniquq_strk = uniquq_strk[uniquq_strk.argsort()]
                    median1 = round(uniquq_strk.shape[0] / 2)
                    strikeDiff = float(uniquq_strk[median1]) - float(uniquq_strk[median1 - 1])
                    contract_fo[np.where(contract_fo[:, 3] == i), 32] = strikeDiff
    ################################################################


    # ############################## future token ##############################
            fltr = np.asarray(['NSEFO'])
            lua = contract_fo[np.in1d(contract_fo[:, 0], fltr)]
            fltr1 = np.asarray(['FUTIDX', 'FUTSTK'])
            lua1 = lua[np.in1d(lua[:, 5], fltr1)]

            # futureTokenDict = {}
            # futurePDict = {}

            for i in unique_symbols:
                if (i != ''):
                    lua2 = lua1[np.in1d(lua1[:, 3], np.asarray([i]))]
                    # print(lua2)
                    xxxx = lua2[lua2[:, 6].argsort()][0][2]
                    contract_fo[np.where(contract_fo[:, 3] == i), 15] = xxxx
                    # futureTokenDict[i] = xxxx

    ####################################################################

    ###################### bhavcopy fo ###################
            a = pd.read_csv('fo_bhav.csv')
            a['EXPIRY_DT'] = a['EXPIRY_DT'].apply(expWork1)
            a['STRIKE_PR'] = a['STRIKE_PR'].apply(strkwork1)
            a['OPTION_TYP'] = a['OPTION_TYP'].replace('XX', ' ')
            b = a.to_numpy()

            c = (b[b[:, 3].argsort()])
            contract_fo = (contract_fo[contract_fo[:, 2].argsort()])

            for i in unique_symbols:
                fltr = np.asarray([i])
                filertDf = contract_fo[np.in1d(contract_fo[:, 3], fltr)]
                filertBC = c[np.in1d(c[:, 1], fltr)]

                for ik in filertBC:
                    xx = filertDf[np.where(filertDf[:, 6] == ik[2])]
                    yy = xx[np.where(xx[:, 7] == ik[3])]
                    if (yy.shape[0] > 1):
                        zz = yy[np.where(yy[:, 8] == ik[4])]
                    else:
                        zz = yy
                    try:
                        contract_fo[zz[0][2] - 35000, 16] = ik[9]
                        contract_fo[zz[0][2] - 35000, 17] = ik[9]
                    except:
                        print('bhavcopy error',ik)

                        pass

    # ############################## future token ##############################
            fltr = np.asarray(['NSEFO'])
            lua = contract_fo[np.in1d(contract_fo[:, 0], fltr)]
            fltr1 = np.asarray(['FUTIDX', 'FUTSTK'])
            lua1 = lua[np.in1d(lua[:, 5], fltr1)]

            futureTokenDict = {}
            futurePDict = {}

            for i in unique_symbols:
                if (i != ''):
                    lua2 = lua1[np.in1d(lua1[:, 3], np.asarray([i]))]
                    xxxx = lua2[lua2[:, 6].argsort()][0][2]
                    contract_fo[np.where(contract_fo[:, 3] == i), 15] = xxxx
                    futureTokenDict[i] = xxxx

    ####################################################################

    ################## working for moneyness ##########################

            for i in unique_symbols:
                if (i != ''):
                    fltr = np.asarray([i])
                    filterDf2 = contract_fo[np.in1d(contract_fo[:, 3], fltr)]

                    tkn = filterDf2[0][15]
                    prce = contract_fo[tkn - 35000, 16]
                    strikeDif1 = contract_fo[tkn - 35000, 32]
                    # print('filterDf2',i,filterDf2[0][15],prce,strikeDif1)
                    atm = int(prce / strikeDif1) * strikeDif1

                    # fltr = np.asarray([i])
                    # filterDf2 = contract_fo[np.in1d(contract_fo[:, 3], fltr)]

                    # fltr2 = np.asarray([Symbol_Expiry_Dict[i][0]])
                    # filterDf3=filterDf2[np.in1d(filterDf2[:, 3], fltr2)]

                    fltr = np.asarray(['CE'])
                    filterDf3 = filterDf2[np.in1d(filterDf2[:, 8], fltr)]

                    atmToken = filterDf3[np.where(filterDf3[:, 12] == atm), 2][0]

                    atmToken1 = np.subtract(atmToken, 35000).tolist()
                    # print(i, atmToken1)
                    contract_fo[atmToken1, 27] = 1

                    otmToken = filterDf3[np.where(filterDf3[:, 12] > atm), 2][0]
                    # print(i, atmToken1)
                    otmToken1 = np.subtract(otmToken, 35000).tolist()
                    contract_fo[otmToken1, 27] = 3

                    itmToken = filterDf3[np.where(filterDf3[:, 12] < atm), 2][0]
                    itmToken1 = np.subtract(itmToken, 35000).tolist()
                    contract_fo[itmToken1, 27] = 5

                    fltr = np.asarray(['PE'])
                    filterDf3 = filterDf2[np.in1d(filterDf2[:, 8], fltr)]

                    atmToken = filterDf3[np.where(filterDf3[:, 12] == atm), 2][0]
                    atmToken1 = np.subtract(atmToken, 35000).tolist()
                    contract_fo[atmToken1, 27] = 2

                    otmToken = filterDf3[np.where(filterDf3[:, 12] < atm), 2][0]
                    otmToken1 = np.subtract(otmToken, 35000).tolist()
                    contract_fo[otmToken1, 27] = 4

                    itmToken = filterDf3[np.where(filterDf3[:, 12] > atm), 2][0]
                    itmToken1 = np.subtract(itmToken, 35000).tolist()
                    contract_fo[itmToken1, 27] = 6

            ########################################################################

            contract_fo1=contract_fo[:,[2,4,8,4]]
            fltr2 = np.asarray(['CE'])
            contract_fo1_ce1 = contract_fo1[np.in1d(contract_fo1[:, 2], fltr2)]
            contract_fo1_ce2 = contract_fo1_ce1[:,[0,1,3]]

            for j,i in enumerate(contract_fo1_ce2):
                if(contract_fo1_ce2[j,1] != ''):
                    contract_fo1_ce2[j,1] = contract_fo1_ce2[j,1][:-2]

            fltr2 = np.asarray(['PE'])
            contract_fo1_pe1 = contract_fo1[np.in1d(contract_fo1[:, 2], fltr2)]
            contract_fo1_pe2 = contract_fo1_pe1[:,[0,1,3]]

            for j,i in enumerate(contract_fo1_pe2):
                if(contract_fo1_pe2[j,1] != ''):
                    contract_fo1_pe2[j,1] = contract_fo1_pe2[j,1][:-2]





            ##################### adding extra columns for additional fields ##########
            bdf = np.zeros((contract_fo.shape[0], 1))
            contract_fo = np.hstack([contract_fo, bdf])
            ################################################################

            contract_fo_pd= pd.DataFrame(contract_fo)
            contract_fo1_ce_pd = pd.DataFrame(contract_fo1_ce2,columns=['a','b','c']).set_index('b')
            contract_fo1_pe_pd = pd.DataFrame(contract_fo1_pe2,columns=['e','b','f']).set_index('b')

            contract_fo1_ce_pd1=contract_fo1_ce_pd.join(contract_fo1_pe_pd).set_index('a')
            contract_fo1_pe_pd1=contract_fo1_pe_pd.join(contract_fo1_ce_pd).set_index('e')



            # print(contract_fo1_ce_pd1.iloc[0,:])
            # print(contract_fo1_pe_pd1)


            for i in contract_fo:
                if(i[8] == 'CE'):
                    contract_fo[i[2]-35000,34] = contract_fo1_ce_pd1.loc[i[2],'e']
            for i in contract_fo:
                if(i[8] == 'PE'):
                    try:
                        contract_fo[i[2]-35000,34] = contract_fo1_pe_pd1.loc[i[2],'a']
                    except:
                        print('error',i[2],contract_fo1_pe_pd1.loc[i[2]])
        ############ option chain master #########################




            fo_contract_df11 = pd.DataFrame(contract_fo)
            payloadsub = {"exchangeSegmentList": ["NSECM"]}
            payloadsubjson = json.dumps(payloadsub)
            req = requests.request("POST", sub_url, data=payloadsubjson, headers=mheaders)
            data_p = req.json()
            abc = data_p['result']

            ContractEQ1 = os.path.join(loc1[0] ,'Resourses','ContractEQ')
            with open(ContractEQ1, 'w') as f:
                f.write(abc)
            f.close()
            contractEq1 = pd.read_csv(ContractEQ1, header=None, sep='|',index_col=False
                                      ,names=['ExchangeSegment',
                                              'Token','InstrumentType','symbol','Stock_name','exp',' NameWithSeries','InstrumentID',
                                              'PriceBand.High','PriceBand.Low','FreezeQty','tick_size','lot_size','Multiplier'])

            contractEq1['Multiplier'] = 1
            contractEq1['Exchange'] = 'NSECM'
            contractEq1['Segment'] = 'E'
            contractEq1['option_type'] = ' '
            contractEq1['strike_price'] =' '
            contractEq1['instrument_type'] =' '
            contractEq1['asset_token'] =' '
            contractEq1['strike1'] =0.0


            # print(contractEq1.loc[:,'FreezeQty'])


            cndEq = contractEq1[['Exchange',
                                 'Segment','Token', 'Stock_name','symbol','instrument_type',
                                 'exp','strike_price','option_type','asset_token','tick_size',
                                 'lot_size','strike1','Multiplier','FreezeQty','PriceBand.High',
                                 'PriceBand.Low']]

            contract_eq = cndEq.to_numpy()

            ##################### indexing by tokens for master EQ #####################
            contract_eq = (contract_eq[contract_eq[:, 2].argsort()])
            strat_point = 0
            end_point = (contract_eq[-1][2])
            gap = end_point - strat_point
            # print(strat_point,end_point,gap)
            temp_df1 = np.arange(start=0, stop=gap + 5000, step=1)
            raw_token = contract_eq[:, 2]
            total_token = np.hstack([raw_token, temp_df1])
            v, r = np.unique(total_token, return_counts=True)
            unique_token = v[np.where(r == 1)]
            temp_rows = np.empty((unique_token.shape[0], 17), dtype=object)
            temp_rows[:, 2] = unique_token
            temp_rows[:, [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,15,16]] = ['NSECM', '', '', '', '', '', '', '', 0.0,
                                                                             0.0, 0, '', 0.0, 0,0.0,0.0]

            contract_eq = np.vstack([contract_eq, temp_rows])
            contract_eq = (contract_eq[contract_eq[:, 2].argsort()])

            ##################### adding extra columns for additional fields ##########
            bdf = np.zeros((contract_eq.shape[0], 21))
            contract_eq = np.hstack([contract_eq, bdf])
            Eq_contract_df1=pd.DataFrame(contract_eq)
            ################################################################






            ##################################################################### #####################################################################
            # payloadsub = {"exchangeSegmentList": ["MCXFO"]}
            # payloadsubjson = json.dumps(payloadsub)
            # req = requests.request("POST", sub_url, data=payloadsubjson, headers=mheaders)
            # data_p = req.json()
            #
            # abc = data_p['result']
            # # print(data_p)
            # ContractMCX1 = os.path.join(loc1[0] ,'Resourses','ContractMCX')
            # with open(ContractMCX1, 'w') as f:
            #     f.write(abc)
            # f.close()
            #
            # contractMCX1 = pd.read_csv(ContractMCX1, header=None, sep='|'
            #                            , names=['ExchangeSegment', 'Token', 'instrument_type1', 'symbol', 'Stock_name',
            #                                     'instrument_type', ' NameWithSeries', 'InstrumentID', 'PriceBand.High',
            #                                     'PriceBand.Low',
            #                                     'FreezeQty', 'tick_size', 'lot_size', 'Multiplier', 'UnderlyingInstrumentId',
            #                                     'IndexName',
            #                                     'ContractExpiration', 'strike1', 'OptionType'])
            #
            #
            # contractMCX1 = contractMCX1[contractMCX1['instrument_type1'] != 4]
            # contractMCX1['Segment'] = contractMCX1['OptionType'].apply(segwork)
            # contractMCX1['option_type'] = contractMCX1['OptionType'].apply(otwork)
            # contractMCX1['Exchange'] = 'MCXFO'
            #
            #
            # contractMCX1['strike1'] = contractMCX1['strike1'].fillna(' ')
            # contractMCX1['strike_price'] = contractMCX1['strike1'].apply(spwork)
            # contractMCX1['exp'] = contractMCX1['ContractExpiration'].apply(expwork)
            # contractMCX1['asset_token'] = contractMCX1['UnderlyingInstrumentId'].apply(assetTokenWork)
            #
            # cndfMCX1 = contractMCX1[['Exchange','Segment','Token', 'symbol', 'Stock_name', 'instrument_type', 'exp', 'strike_price', 'option_type',
            #                      'asset_token', 'tick_size', 'lot_size', 'strike1','FreezeQty']]
            #
            # cndMCX = dt.Frame(cndfMCX1)
        ##########################################################################################################################################


            payloadsub = {"exchangeSegmentList": ["NSECD"]}
            payloadsubjson = json.dumps(payloadsub)
            req = requests.request("POST", sub_url, data=payloadsubjson, headers=mheaders)
            data_p = req.json()
            abc = data_p['result']
            # print(abc)
            contractCD_raw = os.path.join(loc1[0] ,'Resourses','contractCD_raw.txt')
            with open (contractCD_raw,'w') as f:
                f.write(abc)
            f.close()
            contractCD1 = pd.read_csv(contractCD_raw, header=None, sep='|'
                                      ,names=['ExchangeSegment',
                                              'Token', 'instrument_type1', 'symbol', 'Stock_name','instrument_type',
                                              'NameWithSeries', 'InstrumentID', 'PriceBand.High','PriceBand.Low','FreezeQty',
                                              'tick_size', 'lot_size', 'Multiplier',
                                                 'UnderlyingInstrumentId','IndexName','ContractExpiration', 'strike1', 'OptionType'])
            contractCD1 = contractCD1[contractCD1['instrument_type1']!=4]
            contractCD1['Exchange'] = 'NSECD'
            # contractCD1['Segment'] = 'F'
            contractCD1['Segment'] = contractCD1['OptionType'].apply(segwork)
            contractCD1['option_type'] = contractCD1['OptionType'].apply(otwork)
            contractCD1['strike1'] = contractCD1['strike1'].fillna('')
            contractCD1['strike_price'] = contractCD1['strike1'].apply(spwork)
            contractCD1['exp'] = contractCD1['ContractExpiration'].apply(expwork)
            contractCD1['asset_token'] = contractCD1['UnderlyingInstrumentId'].apply(assetTokenWork)
            cndCD1 = contractCD1[['Exchange',
                                  'Segment','Token', 'symbol', 'Stock_name', 'instrument_type',
                                  'exp', 'strike_price', 'option_type','asset_token', 'tick_size',
                                  'lot_size', 'strike1','Multiplier','FreezeQty','PriceBand.High',
                                  'PriceBand.Low']]


            contract_cd = cndCD1.to_numpy()

            ##################### indexing by tokens for master EQ #####################
            contract_cd = (contract_cd[contract_cd[:, 2].argsort()])
            strat_point = 0
            end_point = (contract_cd[-1][2])
            gap = end_point - strat_point
            # print('NSECDS',strat_point,end_point,gap)
            temp_df1 = np.arange(start=0, stop=gap + 5000, step=1)
            raw_token = contract_cd[:, 2]
            total_token = np.hstack([raw_token, temp_df1])
            v, r = np.unique(total_token, return_counts=True)
            unique_token = v[np.where(r == 1)]
            temp_rows = np.empty((unique_token.shape[0], 17), dtype=object)
            temp_rows[:, 2] = unique_token
            temp_rows[:, [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,15,16]] = ['NSECD', '', '', '', '', '', '', '', 0.0,
                                                                             0.0, 0, '', 0.0, 0,0.0,0.0]
            contract_cd = np.vstack([contract_cd, temp_rows])
            contract_cd = (contract_cd[contract_cd[:, 2].argsort()])
            ##################### adding extra columns for additional fields ##########
            bdf = np.zeros((contract_cd.shape[0], 21))
            contract_cd = np.hstack([contract_cd, bdf])
            ################################################################
            cd_contract_df1=pd.DataFrame(contract_cd)
            cndfCD = dt.Frame(cndCD1)
            ##########################################################################################################################################
            heads = ['Exchange',
                     'Segment', 'Token', 'symbol', 'Stock_name', 'instrument_type',
                     'exp', 'strike_price', 'option_type', 'asset_token', 'tick_size',
                     'lot_size', 'strike1', 'Multiplier', 'FreezeQty','pbHigh',
                     'pbLow', 'futureToken','close', 'ltp','bid',
                     'ask', 'oi','prev_day_oi', 'iv','delta',
                     'gamma', 'theta','vega', 'theoritical_price','PPR',
                     'moneyness', 'Volume','amt', 'Avg1MVol','ATP',
                     'strike_diff', 'expiry_type','cpToken'
                     ]
            contract_df = pd.concat([fo_contract_df11,Eq_contract_df1,cd_contract_df1])
            contract_df.columns = heads
            # contract_df['instrument_type'] =  contract_df['instrument_type'].fillna('Equity')
            contract_df1=os.path.join(loc1[0],'Resourses','ContractMasters','contract_df.csv')
            contract_df.to_csv(contract_df1,index=False)
            contract_full = contract_df.to_numpy()
            return contract_full,contract_fo,contract_eq,contract_cd,heads
        else:
            contract_df = pd.read_csv(contractDir,low_memory=False,)
            contract_dt = dt.Frame(contract_df)
            contract_full = contract_dt.to_numpy()
            heads = ['Exchange',
                     'Segment', 'Token', 'symbol', 'Stock_name', 'instrument_type',
                     'exp', 'strike_price', 'option_type', 'asset_token', 'tick_size',
                     'lot_size', 'strike1', 'Multiplier', 'FreezeQty','pbHigh',
                     'pbLow', 'futureToken','close', 'ltp','bid',
                     'ask', 'oi','prev_day_oi', 'iv','delta',
                     'gamma', 'theta','vega', 'theoritical_price','PPR',
                     'moneyness', 'Volume','amt', 'Avg1MVol','ATP',
                     'strike_diff', 'expiry_type','cpToken'
                     ]
            fltr2 = np.asarray(['NSEFO'])
            contract_fo = contract_full[np.in1d(contract_full[:, 0], fltr2)]
            fltr2 = np.asarray(['NSECM'])
            contract_eq = contract_full[np.in1d(contract_full[:, 0], fltr2)]
            fltr2 = np.asarray(['NSECD'])
            contract_cd = contract_full[np.in1d(contract_full[:, 0], fltr2)]

            ############ option chain master #########################
            return contract_full,contract_fo,contract_eq,contract_cd,heads

    except:
        print(traceback.print_exc())
        print(sys.exc_info(), "@download master")



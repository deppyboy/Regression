import pyodbc, datetime, csv, sqlite3
import fund, streams, os, xlrd

DBLOC = 'j:\\valuation\\output\\'
PWD = 'tdees_3'
DBNAME = 'OracleDB'
ORACLESTRING = 'DSN=%s;PWD=%s' % (DBNAME, PWD)

FILENAME = 'H:\dat\PFUVFILE.TXT'
#SQLLITEDB = 'c:\\sqlite\\funddata2.db'
SQLLITEDB = 'c:\\temp\\funddata.db'

def importdata(filename=FILENAME,sqllitedb=SQLLITEDB):
    conn = sqlite3.connect(sqllitedb)
    c = conn.cursor()
    try:
        c.execute('DROP TABLE funds;')
    except:
        pass
    c.execute('CREATE TABLE funds (company INTEGER, mnemonic TEXT, date TEXT, fundnum INTEGER, nav REAL);')
    f = open(filename)
    row = f.readline()
    while row:
        if row[0]=='U':
            company = int(row[1:4])
            mnemonic = row[4:12].strip()
            date = row[12:16]+'-'+row[16:18]+'-'+row[18:20]
            fundcount = int(row[20:23])
            for i in range(0,fundcount):
                fundnum = int(row[23+i*12:23+i*12+3])
                nav=int(row[26+i*12:26+i*12+9])/1000000.0
                sql = "INSERT INTO funds VALUES(%s, '%s', '%s', %s, %s);" % (str(company), mnemonic, date, str(fundnum),str(nav))
                c.execute(sql)
        row = f.readline()
    c.execute('CREATE UNIQUE INDEX pkey on funds(company ASC,mnemonic ASC, fundnum ASC, date ASC);')
    conn.commit()
    conn.close()


def oracledatebuilder(mydate):
    return "TO_DATE('"+mydate.strftime('%Y%m%d')+"','yyyymmdd')"

def getmarketreturns(startdate,enddate):
    marketdata = streams.getmarketdatadb(ORACLESTRING)
    indexnames = ['SPX','AGG','EAFE','RTY','TBILL']
    returndict = {}
    for index in indexnames:
        start, end = marketdata[index].dayquote(startdate),marketdata[index].dayquote(enddate)
        returndict[index] = end/start-1.0
    return returndict

def calcspxpct(evaldate):
    cnxn = pyodbc.connect(ORACLESTRING)
    c = cnxn.cursor()
    sql = "SELECT Sum(CASH) as BILL, Sum(BOND) as BND, Sum(SMALL_CAP) as RTY, Sum(LARGE_CAP) as SPX, "
    sql += "Sum(INTERNATIONAL) as EAFE, Sum(FIXED) as FXD, Sum(DCA_PLUS) as DCA FROM ODSACT.ACT_RSL_SERIATIM WHERE "
    sql += "GENERATION_DATE="+oracledatebuilder(evaldate)+" GROUP BY GENERATION_DATE;"
    c.execute(sql)
    row = c.fetchone()
    cnxn.close()
    return row.SPX / (row.BILL + row.BND + row.RTY + row.SPX+row.EAFE+row.FXD+row.DCA)

def getdelta(deltadate):
    shocklocation = '\\\\anpdnas1\\chp_prod$\\Trading\Model\\'+str(deltadate.year)+'\\'+deltadate.strftime('%Y%m')+'\\'+deltadate.strftime('%Y%m%d')+'\\'
    files = os.listdir(shocklocation)
    filename = filter(lambda x: x[-3:]=='xls', files)[0]
    wb = xlrd.open_workbook(shocklocation+filename)
    sht = wb.sheet_by_name('VAHA_Output')
    startval = 2 if sht.cell(0,1).value=='CSA' else 1
    upshock = sum([sht.cell(2,i).value for i in range(startval,startval+6)])
    downshock = sum([sht.cell(3,i).value for i in range(startval,startval+6)])
    spxdelta = -(upshock-downshock)/2.0
    scalefactor = calcspxpct(deltadate)
    return spxdelta/scalefactor
    
def loadfundamounts(funddate):
    cnxn = pyodbc.connect(ORACLESTRING)
    cursor = cnxn.cursor()
    sql = "SELECT PRODUCT_FUND_ID, Sum(Fund_Val) AS FundTotal FROM ODSACT.ACT_PRC_CONTRACT_FUND_VALUE WHERE GENERATION_DATE="
    sql += oracledatebuilder(funddate) + " AND GENERATION_TYPE='W' GROUP BY PRODUCT_FUND_ID;"
    cursor.execute(sql)
    fundnum, mnemonics, fundamt = [], [], []
    for row in cursor.fetchall():
        if fundamt!=0.0:
            fundnum.append(int(row[0][-3:]))
            mnemonics.append(row[0][0:-3])
            fundamt.append(float(row[1]))
    transmnemonic = {}
    cnxn.close()
    #CHANGE DBs
    cnxn = sqlite3.connect(SQLLITEDB)
    c = cnxn.cursor()    
    sql = 'select * from trans;'
    c.execute(sql)
    for row in c.fetchall():
        transmnemonic[row[0]] = row[1]
    for i in range(0,len(mnemonics)):
        if mnemonics[i] in transmnemonic.keys():
            mnemonics[i] = transmnemonic[mnemonics[i]]
    cnxn.close()
    return fundnum, mnemonics, fundamt

def loadhighlevelfundamounts(funddate):
    cnxn = pyodbc.connect(ORACLESTRING)
    cursor = cnxn.cursor()
    sql = "SELECT SUBSTR(PRODUCT_FUND_ID,-3), Sum(Fund_Val) AS FundTotal FROM ODSACT.ACT_PRC_CONTRACT_FUND_VALUE WHERE GENERATION_DATE="
    sql += oracledatebuilder(funddate) + " AND GENERATION_TYPE='W' GROUP BY SUBSTR(PRODUCT_FUND_ID,-3);"
    cursor.execute(sql)
    fundnum, fundamt = [], []
    for row in cursor.fetchall():
        if fundamt!=0.0:
            fundnum.append(int(row[0]))
            fundamt.append(float(row[1]))
    cnxn.close()
    return fundnum, fundamt
    

def getmappings(date):
    cnxn = pyodbc.connect(ORACLESTRING)
    c=cnxn.cursor()
    datestring = oracledatebuilder(date)
    sql = "SELECT * FROM ODSACT.ACT_SRC_FUND_MAPPING WHERE START_DATE<="+datestring
    sql += " AND END_DATE>="+datestring + ";"
    c.execute(sql)
    mappings,trans = {}, {}
    for row in c.fetchall():
        if row.CASH!=None:
            mappings[int(row.FUND_NO)] = {  'TBILL' : float(row.CASH),
                                            'AGG' : float(row.BOND),
                                            'RTY' : float(row.SMALL_CAP),
                                            'SPX' : float(row.LARGE_CAP),
                                            'EAFE' : float(row.INTERNATIONAL)}
            trans[int(row.FUND_NO)] = row.FUND_DESC
    cnxn.close()
    return mappings,trans

def calchlfundperformance(startdate,enddate):
    fundnums, fundamts = loadhighlevelfundamounts(startdate)
    delta = getdelta(startdate)
    returns = getmarketreturns(startdate,enddate)
    mappings,trans = getmappings(startdate)
    conn = sqlite3.connect(SQLLITEDB)
    c = conn.cursor()
    try:
        sql = "DELETE FROM fundperf WHERE startdate='"+startdate.strftime('%Y-%m-%d')+"' and enddate='"+enddate.strftime('%Y-%m-%d')+"';"
        c.execute(sql)
    except:
        sql = 'CREATE TABLE fundperf(mnemonic TEXT, fundnum INTEGER, startdate TEXT, enddate TEXT, fundname TEXT, fundamount REAL, actual REAL, expected REAL, diff REAL, delta REAL, pl REAL);'
        c.execute(sql)    
    totalamt = 0.0
    for mynum, myamt in zip(fundnums,fundamts):
        if mynum in mappings.keys():
            myfund = fund.companycodefinder(mnemonic='',code=mynum,mapping=mappings[mynum],freq='D')
            if myfund:
                totalamt += myamt
                actual=myfund.actualreturn(startdate,enddate)
                expected=myfund.projectedreturn(returns)
                diff=actual-expected
                sql = "INSERT INTO fundperf VALUES('',"+str(mynum)+",'"+startdate.strftime('%Y-%m-%d')+"','"+enddate.strftime('%Y-%m-%d')+"','"+trans[mynum]+"',"+str(myamt)+","+str(actual)+","+str(expected)+","+str(diff)+",0.0,0.0);"
                c.execute(sql)
    sql = "SELECT * from fundperf WHERE startdate='"+startdate.strftime('%Y-%m-%d')+"' AND enddate='"+enddate.strftime('%Y-%m-%d')+"';"
    c.execute(sql)
    for row in c.fetchall():
        deltaalloc = row[5]/totalamt * delta
        pl = deltaalloc * row[8]/.01
        sql = "UPDATE fundperf SET delta="+str(deltaalloc)+", pl=" + str(pl)+" WHERE mnemonic='"+row[0]+"' AND fundnum="+str(row[1])+" AND startdate='"+startdate.strftime('%Y-%m-%d')+"' AND enddate='"+enddate.strftime('%Y-%m-%d')+"';"
        c.execute(sql)
    conn.commit()
    conn.close()


def calcfundperformance(startdate,enddate):
    fundnums, mnemonics, fundamts = loadfundamounts(startdate)
    delta = getdelta(startdate)
    returns = getmarketreturns(startdate,enddate)
    mappings,trans = getmappings(startdate)
    conn = connect(SQLLITEDB)
    c = conn.cursor()
    sql = "DELETE FROM fundperf WHERE startdate='"+startdate.strftime('%Y-%m-%d')+"' and enddate='"+enddate.strftime('%Y-%m-%d')+"';"
    c.execute(sql)
    totalamt = 0.0
    for mynum, mymne, myamt in zip(fundnums,mnemonics,fundamts):
        if mynum in mappings.keys():
            myfund = fund.companycodefinder(mnemonic=mymne,code=mynum,mapping=mappings[mynum],freq='D')
            if myfund:
                totalamt += myamt
                actual=myfund.actualreturn(startdate,enddate)
                expected=myfund.projectedreturn(returns)
                diff=actual-expected
                sql = "INSERT INTO fundperf VALUES('"+mymne+"',"+str(mynum)+",'"+startdate.strftime('%Y-%m-%d')+"','"+enddate.strftime('%Y-%m-%d')+"','"+trans[mynum]+"',"+str(myamt)+","+str(actual)+","+str(expected)+","+str(diff)+",0.0,0.0);"
                c.execute(sql)
    sql = "SELECT * from fundperf WHERE startdate='"+startdate.strftime('%Y-%m-%d')+"' AND enddate='"+enddate.strftime('%Y-%m-%d')+"';"
    c.execute(sql)
    for row in c.fetchall():
        deltaalloc = row[5]/totalamt * delta
        pl = deltaalloc * row[8]/.01
        sql = "UPDATE fundperf SET delta="+str(deltaalloc)+", pl=" + str(pl)+" WHERE mnemonic='"+row[0]+"' AND fundnum="+str(row[1])+" AND startdate='"+startdate.strftime('%Y-%m-%d')+"' AND enddate='"+enddate.strftime('%Y-%m-%d')+"';"
        c.execute(sql)
    conn.commit()
    conn.close()

def outputall(filename='out.csv'):
    conn = sqlite3.connect(SQLLITEDB)
    c = conn.cursor()
    sql = 'select * from fundperf;'
    c.execute(sql)
    output = "Mnemonic,Fund Number,Start Date,End Date,Fund Name,AV,Actual,Expected,Diff,Delta,P&L\n"
    for row in c.fetchall():
        stringrow = [str(field) for field in row]
        output += ",".join(stringrow)+"\n"
    f = open(filename,'w')
    f.write(output)
    f.close()
    conn.close()
    
if __name__=='__main__':
    """dates =[(datetime.datetime(2012,10,12),datetime.datetime(2012,10,19)),
            (datetime.datetime(2012,10,19),datetime.datetime(2012,10,26)),
            (datetime.datetime(2012,10,26),datetime.datetime(2012,11,2)),
            (datetime.datetime(2012,11,2),datetime.datetime(2012,11,9)),
            (datetime.datetime(2012,11,9),datetime.datetime(2012,11,16)),
            (datetime.datetime(2012,11,16),datetime.datetime(2012,11,23)),
            (datetime.datetime(2012,11,23),datetime.datetime(2012,11,30)),
            (datetime.datetime(2012,11,30),datetime.datetime(2012,12,7)),
            (datetime.datetime(2012,12,7),datetime.datetime(2012,12,14)),
            (datetime.datetime(2012,12,14),datetime.datetime(2012,12,21)),
            (datetime.datetime(2012,12,21),datetime.datetime(2012,12,28)),
            (datetime.datetime(2012,12,28),datetime.datetime(2013,1,4)),
            (datetime.datetime(2013,1,4),datetime.datetime(2013,1,11)),
            (datetime.datetime(2013,1,11),datetime.datetime(2013,1,18)),
            (datetime.datetime(2013,1,18),datetime.datetime(2013,1,25)),
            (datetime.datetime(2013,1,25),datetime.datetime(2013,2,1)),
            (datetime.datetime(2013,2,1),datetime.datetime(2013,2,8)),
            (datetime.datetime(2013,2,8),datetime.datetime(2013,2,15)),
            (datetime.datetime(2013,2,15),datetime.datetime(2013,2,22)),
            (datetime.datetime(2013,2,22),datetime.datetime(2013,3,1)),
            (datetime.datetime(2013,3,1),datetime.datetime(2013,3,8)),
            (datetime.datetime(2013,3,8),datetime.datetime(2013,3,15)),
            (datetime.datetime(2013,3,15),datetime.datetime(2013,3,22)),
            (datetime.datetime(2013,3,22),datetime.datetime(2013,3,28)),
            (datetime.datetime(2013,3,28),datetime.datetime(2013,4,5)),
            (datetime.datetime(2013,4,5),datetime.datetime(2013,4,12)),
            (datetime.datetime(2013,4,12),datetime.datetime(2013,4,19)),
            (datetime.datetime(2013,4,19),datetime.datetime(2013,4,26)),
            (datetime.datetime(2013,4,26),datetime.datetime(2013,5,3)),
            (datetime.datetime(2013,5,3),datetime.datetime(2013,5,10)),
            (datetime.datetime(2013,5,10),datetime.datetime(2013,5,17)),
            (datetime.datetime(2013,5,17),datetime.datetime(2013,5,24))]
    
    dates =[(datetime.datetime(2013,5,24),datetime.datetime(2013,5,31))]"""
    initialdate = raw_input('Input start date (YYYYMMDD): ')
    enddate = raw_input('Input end date (YYYYMMDD): ')
    #DBNAME = raw_input('Input Oracle DB name: ')
    #PWD = raw_input('Input Oracle DB password: ')
    PWD = 'tdees_2'
    DBNAME = 'OracleDB'
    ORACLESTRING = 'DSN=%s;PWD=%s' % (DBNAME, PWD)
    strp = lambda k : datetime.datetime.strptime(k,'%Y%m%d')
    dates = [(strp(initialdate),strp(enddate))]
    importdata()
    #for mydate in dates:
        #print 'RUNNING: ' + mydate[1].strftime('%Y%m%d')
        #calchlfundperformance(mydate[0],mydate[1])
    #outputall()
    #print getmarketreturns(datetime.datetime(2012,12,14),datetime.datetime(2012,12,21))
    

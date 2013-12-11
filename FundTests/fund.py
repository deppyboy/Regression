import scipy
import os
import pylab
import datetime
import streams
import scipy.optimize
import xlrd
import pyodbc

DELTACACHE = {} #initialization of cache for cachedelta function
AVCACHE = {} #initialization of cache for avcache function
FILENAME = 'H:\dat\PFUVFILE.TXT' #location of fund data file
ORACLESTRING = streams.ORACLESTRING

def calcspxpct(evaldate):
    """
    Calculates the percentage of account value allocated to SPX as of evaldate.
    """
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
    """
    Calculates the delta for a 1% shock to the account value as of deltadate.
    """
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

def oracledatebuilder(mydate):
    """Converts a Python datetime to an Oracle date string."""
    return "TO_DATE('"+mydate.strftime('%Y%m%d')+"','yyyymmdd')"

def importdata(filename=FILENAME,dbstring=ORACLESTRING,baseonly=True):
    """
    Imports fund data into the Oracle database.
    
    Parameters
    ----------
    filename : string (default FILENAME)
        filename of text file to parse 
    dbstring : string (default ORACLESTRING)
        ODBC connection string for database
    baseonly : bool (default True)
        if True, only import the base NAV,
        and the NAV for the PNDY mnemonic;
        otherwise, import all records (slow)
    """
    conn = pyodbc.connect(dbstring)
    c = conn.cursor()
    c.execute('delete from funddata;')
    f = open(filename)
    row = f.readline()
    while row:
        if row[0]=='U':
            company = int(row[1:4])
            mnemonic = row[4:12].strip()
            if mnemonic == '': mnemonic='BASENAV'
            date = datetime.datetime(int(row[12:16]),int(row[16:18]),int(row[18:20]))
            date = oracledatebuilder(date)
            fundcount = int(row[20:23])
            for i in range(0,fundcount):
                fundnum = int(row[23+i*12:23+i*12+3])
                nav=int(row[26+i*12:26+i*12+9])/1000000.0
                sql = "INSERT INTO funddata VALUES(%s, '%s', %s, %s, %s);" % (str(company), mnemonic, date, str(fundnum),str(nav))
                if baseonly and (mnemonic=='BASENAV' or mnemonic=='PNDY'):
                	c.execute(sql)
                elif not(baseonly):
					c.execute(sql)
        row = f.readline()
    conn.commit()
    conn.close()

def cachedelta(date):
    """Cached wrapper for getdelta."""
    if date in DELTACACHE.keys():
		return DELTACACHE[date]
    else:
        DELTACACHE[date] = getdelta(date)
        return DELTACACHE[date]

def avcache(funddate):
    """Cached function to get the total AV as of funddate"""
    if funddate in AVCACHE.keys():
		return AVCACHE[funddate]
    else:
        cnxn = pyodbc.connect(ORACLESTRING)
        cursor = cnxn.cursor()
        sql = "SELECT Sum(Fund_Val) AS FundTotal FROM ODSACT.ACT_PRC_CONTRACT_FUND_VALUE WHERE GENERATION_DATE="
        sql += oracledatebuilder(funddate) + " AND GENERATION_TYPE='W';"
        cursor.execute(sql)
        row = cursor.fetchone()
        AVCACHE[funddate] = float(row[0])
        cnxn.close()
        return AVCACHE[funddate]

class Fund:
    def __init__(self, company, mnemonic, fundcode, mapping=None,
                 freq='D', forcedates=True, asofdate=datetime.datetime.now()):
        """
        Constructor for fund class.
        
        Parameters
        ----------
        company : 101 or 111
            indicates PL or PL&A
        mnemonic : string
            product code - BASENAV for
            pure NAVs
        fundcode : int
            internal code for fund
        mapping : dict (default None)
            if None, mapping is pulled from database,
            otherwise mapping can be specified.
        freq : char (default 'D')
            frequency of return calc for fund.
            'D' for daily, 'W' for weekly, 'M' for monthly
        forcedates : bool (default True)
            if changefreq != 'D' then forcedates will ensure
            that a return is reported on a weekly/monthly basis.
        asofdate : datetime (default now())
            as of date for mappings
        """
        conn = pyodbc.connect(ORACLESTRING)
        c = conn.cursor()
        sql = "SELECT * from funddata WHERE company=%s and mnemonic='%s' and fundnum=%s ORDER BY navdate;" % (str(company),mnemonic,str(fundcode))
        c.execute(sql)
        rows = c.fetchall()
        dates = [row[2] for row in rows]
        navs = [row[4] for row in rows]
        self.stream = streams.BasicStream(dates,scipy.asarray(navs))
        if freq!='D': self.stream = self.stream.changefreq(freq,forcedates=forcedates)
        self.freq = freq
        if mapping:
            self.mapping = mapping
        else:
            sql = 'SELECT * FROM ODSACT.ACT_SRC_FUND_MAPPING WHERE FUND_NO='+str(fundcode)+';'
            c.execute(sql)
            for row in c.fetchall():
                if asofdate<row.END_DATE and asofdate>=row.START_DATE:
                    self.mapping = {'TBILL' : float(row.CASH),
                                    'AGG'   : float(row.BOND),
                                    'RTY'   : float(row.SMALL_CAP),
                                    'SPX'   : float(row.LARGE_CAP),
                                    'EAFE'  : float(row.INTERNATIONAL)}
        if not('mapping' in dir(self)):
            self.mapping = None
        self.company = company
        self.mnemonic = mnemonic
        self.fundcode = fundcode
        self.plot = self.stream.plot
        conn.close()

    def backtest(self, trainstart, trainend, backteststart, backtestend, mktbasket):
        """
        Regress and then backtest the regression.
        
        Parameters
        ----------
        trainstart : datetime
            beginning of regression period
        trainend : datetime
            end of regression period
        backteststart : datetime
            beginning of backtesting period
        backtestend : datetime
            end of backtesting period
        mktbasket : dict
            dictionary of market streams
            
        Returns
        -------
        dictionary of statistics for backtesting period
        """
        self.regress(trainstart,trainend,mktbasket)
        return self.stats(backteststart,backtestend,mktbasket) 
    
    def getNAVInfofromdb(self,tablename,mnemonicstring,oraclestring,company,fundcode):
        sql = ("SELECT * from %s WHERE company=%s and mnemonic='%s' and fundnum=%s ORDER BY navdate;") \
                                                                   % (tablename,str(company),mnemonicstring,str(fundcode))
       
        conn = pyodbc.connect(oraclestring)
        c = conn.cursor()
        c.execute(sql)
        column_names = [row[0] for row in c.description]
        datesbase =[]    #need to be removed if we revert back to c.fetchall()
        navsbase =[]    
        rows = c.fetchmany(100)
        navdateindex = column_names.index('NAVDATE')
        navindex = column_names.index('NAV')
        
        while rows:      
            #datesbase.append([row[navdateindex] for row in rows])  #2
            datesbase.extend([row[navdateindex] for row in rows])
            navsbase.extend([row[navindex] for row in rows])        #4 was append prior
            rows = c.fetchmany(100)
        return datesbase,navsbase
   
    def plotreturns(self):
        """Plots actual accumulated fund returns."""
        pylab.plot(self.stream.enddates,scipy.cumprod(1.0+self.stream.returns))
    
    def av(self, date):
        """Returns the fund's AV for a given date."""
        cnxn = pyodbc.connect(ORACLESTRING)
        cursor = cnxn.cursor()
        sql = "SELECT Sum(Fund_Val) AS FundTotal FROM ODSACT.ACT_PRC_CONTRACT_FUND_VALUE WHERE GENERATION_DATE="
        sql += oracledatebuilder(date) + " AND GENERATION_TYPE='W' AND FUND_NO='"+str(self.fundcode) + "';"
        cursor.execute(sql)
        row = cursor.fetchone()
        cnxn.close()
        if row[0]:
            return float(row[0])
        else:
            return 0.0

    def deltaestimate(self, date):
        """Estimates the fund's delta on a date."""
        return cachedelta(date)*self.av(date)/avcache(date)

    def align(self,startdate,enddate,mktbasket):
        """
        Aligns the funds returns with the market returns
        to ensure that there are the same number of returns
        for all indices, and that they are properly aligned.
        
        Parameters
        ----------
        startdate : datetime
            beginning of alignment period
        enddate : datetime
            end of alignment period
        mktbasket : dict
            dictionary of market streams
        
        Returns
        -------
        inputmatrix : 2-d array
            matrix that holds the aligned
            index returns
        fundreturns : 1-d array
            holds the aligned fund returns
        indexes : list
            a list of the indexes in inputmatrix
        daterange : list
            beginning and end dates of the aligned
            data
        
        Note
        ----
        Will return None, None, None, None if
        no data is available between startdate and enddate.
        """
        hybridstream,indexes = [],[]
        for index in mktbasket.keys():
            hybridstream.append(mktbasket[index][startdate:enddate])
            indexes.append(index)
        getdates = self.stream.overlap(hybridstream)
        if not(getdates) or len(getdates)<3:
            return None, None, None, None        daterange = [getdates[0], getdates[-1]]
        fundreturns = self.stream.datereturns(getdates).returns
        indexreturns = [indexstream.datereturns(getdates).returns for indexstream in hybridstream]
        inputmatrix = scipy.vstack(indexreturns).T
        fundreturns = fundreturns.reshape(fundreturns.size,1)
        return inputmatrix, fundreturns, indexes, daterange

    def regress(self, startdate, enddate, mktbasket):
        """
        Regresses a fund against the market indices.
        
        Parameters
        ----------
        startdate : datetime
            beginning of regression period
        enddate : datetime
            end of regression period
        mktbasket : dict
            dictionary of market streams
        
        Returns
        -------
        mapping : dict
            new mapping
        
        Side Effects
        ---- -------
        Also pushes the mapping into the class.
        """
        inputmatrix, fundreturns, indexes, daterange = self.align(startdate, enddate, mktbasket)
        if inputmatrix is None:
            self.mapping = None
            return None        def SSE(beta):
            return scipy.sum((scipy.dot(inputmatrix,beta.reshape(len(indexes),1))-fundreturns)**2.0)
        sumconstraint = lambda beta : 1.0-sum(beta)        guess = scipy.asarray([1.0] + [0.0] * (len(indexes)-1))
        bounds = [(0.0,1.0) for i in range(0,len(indexes))]
        finalbeta = scipy.optimize.fmin_slsqp(SSE,guess,eqcons=[sumconstraint],bounds=bounds,iprint=0,acc=1E-20)
        self.mapping = {}
        for i in range(0,len(indexes)):
            self.mapping[indexes[i]] = finalbeta[i]
        return self.mapping

    def stats(self, startdate, enddate, mktbasket, output = False):
        """
        Calculates statistics for a fund over a period.
        
        Parameters
        ----------
        startdate : datetime
            beginning of statistic period
        enddate : datetime
            end of statistic period
        mktbasket : dict
            dictionary of market streams
        output : bool
            if True, output results to db
        
        Returns
        -------
        stats : dict
            dictionary of statistics
        """
        inputmatrix, fundreturns, indexes, daterange = self.align(startdate, enddate, mktbasket)
        if self.mapping and not(inputmatrix is None):
            weights = scipy.array([self.mapping[mykey] if mykey in self.mapping else 0.0 for mykey in mktbasket.keys()])
            projected = scipy.dot(inputmatrix,weights.reshape(len(indexes),1)).flatten()
            actual = fundreturns.flatten()
            diff = actual-projected
            outdata = {
                     'TE'     : scipy.std(diff)*100.0*100.0,
                     'BETA'   : scipy.cov(projected,actual)[1,0]/scipy.var(projected),
                     'ALPHA'  : (scipy.product(diff+1.0))**(1.0/diff.size)-1.0,
                     'VOL'    : scipy.std(actual)*scipy.sqrt(252.0),
                     'PROJ'   : scipy.product(1.0+projected)-1.0,
                     'ACT'    : scipy.product(1.0+actual)-1.0,
                     'R2'     : 0.0 if scipy.all(actual==0.0) else scipy.corrcoef(projected,actual)[1,0]**2.0,
                     'AV'     : self.av(startdate),
                     'DELTA'  : self.deltaestimate(startdate)
                    }
            outdata['DIFF'] = outdata['ACT']-outdata['PROJ']
            outdata['PL'] = outdata['DELTA']*outdata['DIFF']*100.0 
            if output:
                cnxn = pyodbc.connect(ORACLESTRING)
                cursor = cnxn.cursor()
                sql = 'INSERT INTO FUNDOUTPUT VALUES ({0!s},{1!s},{2!s},{3!s},{4!s},{5!s},{6},{7},{8!s},{9!s},{10!s},{11!s},{12!s},{13!s});'
                sql = sql.format(self.fundcode,outdata['PROJ'],outdata['ACT'],outdata['DIFF'],
                           outdata['DELTA'],outdata['PL'],oracledatebuilder(startdate),
                           oracledatebuilder(enddate),outdata['TE'],outdata['R2'],outdata['BETA'],
                           outdata['ALPHA'],outdata['VOL'],outdata['AV'])
                cursor.execute(sql)
                cnxn.commit()            
                cnxn.close()
            return outdata
        else:
            return None
    
    def error(self,startdate,enddate,threshold=0.03):
        """
        Checks for possible erroneous returns.
        
        Parameters
        ----------
        startdate : datetime
            beginning of check period
        enddate : datetime
            end of check period
        threshold : float (default 0.03)
            threshold to return an error
        
        Returns
        -------
        err : bool
            if an error occurs, err is True,
            else it's False.
        errs : list
            a list of (date,return) tuples
            that exceed the threshold
        """
        dt,errs,err = startdate, [], False
        while dt<=enddate:
            ret = self.stream[dt]
            if ret:
                if abs(ret)>threshold:
                    err = True
                    errs.append((dt,ret))
            dt+=datetime.timedelta(days=1)
        return err, errs
            
class AdjFund(Fund):
    def __init__(self,fundcode,company=101,mapping=None,freq='D',forcedates=True,asofdate=datetime.datetime.now()):
        """
        Constructor for fund class.
        
        Parameters
        ----------
        fundcode : int
            internal code for fund
        company : 101 or 111 (default 101)
            indicates PL or PL&A
        mapping : dict (default None)
            if None, mapping is pulled from database,
            otherwise mapping can be specified.
        freq : char (default 'D')
            frequency of return calc for fund.
            'D' for daily, 'W' for weekly, 'M' for monthly
        forcedates : bool (default True)
            if changefreq != 'D' then forcedates will ensure
            that a return is reported on a weekly/monthly basis.
        asofdate : datetime (default now())
            as of date for mappings
        
        Note
        ----
        Only difference for this is that it replaces bad returns from the basenavs
        with the NAVs from PNDY.  This is admittedly a hack, and I need to think of
        a better solution in the future.
        """
        mnemonics = ['BASENAV','PNDY']
        FundDBTable = 'tdees.funddata'
        datesbase,navsbase = self.getNAVInfofromdb(FundDBTable,mnemonics[0],ORACLESTRING,company,fundcode)
        datespndy,navspndy = self.getNAVInfofromdb(FundDBTable,mnemonics[1],ORACLESTRING,company,fundcode)
        self.stream = streams.hybridstream(datesbase,datespndy,scipy.array(navsbase),scipy.array(navspndy))
        if freq!='D': self.stream = self.stream.changefreq(freq,forcedates=forcedates)
        self.freq = freq
        if mapping:
            self.mapping = mapping
        else:
            sql = 'SELECT * FROM ODSACT.ACT_SRC_FUND_MAPPING WHERE FUND_NO='+str(fundcode)+';'
            c.execute(sql)
            for row in c.fetchall():
                if asofdate<row.END_DATE and asofdate>=row.START_DATE:
                    self.mapping = {'TBILL' : float(row.CASH),
                                    'AGG'   : float(row.BOND),
                                    'RTY'   : float(row.SMALL_CAP),
                                    'SPX'   : float(row.LARGE_CAP),
                                    'EAFE'  : float(row.INTERNATIONAL)}
        if not('mapping' in dir(self)):
            self.mapping = None
        self.company = company
        self.mnemonic = 'ADJ'
        self.fundcode = fundcode
        self.plot = self.stream.plot
        conn.close()

        
#these are the funds that aren't available for PNDY.  See the AdjFund Note for more detail.
basefunds = [825,826,827,850,851,852,853,875,876,877,878,879,880,881,884,885,886,887,888,923,995]
        
def graballandoutput(startdate,enddate,mktbasket,asofdate=datetime.datetime.now()):
    """
    Loads in all of the funds and runs statistics on them.
    Outputs results to the database.
    
    Parameters
    ----------
    startdate : datetime
        beginning of statistic period
    enddate : datetime
        end of statistic period
    mktbasket : dict
        dictionary of market streams
    asofdate : datetime (default now())
        date of mappings
    """
    cnxn = pyodbc.connect(ORACLESTRING)
    cursor = cnxn.cursor()
    sql = 'delete from fundoutput;'
    cursor.execute(sql)
    cnxn.commit()
    sql = 'select fundnum from funddata group by fundnum;'
    cursor.execute(sql)
    fundnums = [int(row[0]) for row in cursor.fetchall()]
    for fundnum in fundnums:
        print fundnum
        if fundnum in basefunds:
            f = Fund(101,'BASENAV',fundnum,asofdate=asofdate)
        else:
            f = AdjFund(fundnum,asofdate=asofdate) #changed it out, obviously
        f.stats(startdate, enddate, mktbasket, output=True)
    cnxn.close()

def errreport(startdate, enddate, threshold=0.03):
    """
    Runs the error report on all funds and outputs to the database.
    
    Parameters
    ----------
    startdate : datetime
        beginning of error period
    enddate : datetime
        end of error period
    threshold : float (default 0.03)
        threshold to detect errors
    """
    cnxn = pyodbc.connect(ORACLESTRING)
    cursor = cnxn.cursor()
    cursor.execute('delete from funderrors;')
    cursor.commit()
    sql = 'select fundnum from funddata group by fundnum;'
    cursor.execute(sql)
    fundnums = [int(row[0]) for row in cursor.fetchall()]
    for fundnum in fundnums:
        print fundnum
        if fundnum in basefunds:
            f = Fund(101,'BASENAV',fundnum)
        else:
            f = AdjFund(fundnum)
        errval = f.error(startdate,enddate,threshold=threshold)
        if errval[0]:
            for myval in errval[1]:
                sql = 'insert into funderrors values('+str(fundnum)+','+oracledatebuilder(myval[0])+','+str(myval[1])+');'
                cursor.execute(sql)
    cursor.commit()
    cnxn.close()
    
if __name__=='__main__':
    mkt = streams.getmarketdatadb()
    startdt = datetime.datetime(2013,6,28)
    enddt = datetime.datetime(2013,9,27)
    f = AdjFund(964) #nothing special about 964. its a place holder
    f.stats(startdt,enddt,mkt)
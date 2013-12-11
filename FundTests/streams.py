import csv
import datetime
import scipy
import pylab
import pyodbc

#This Login Info is being referenced as a static global name. Need to wrap a namespace so as to ascribe context
PWD =  'pmanikonda_99'
DBNAME = 'OracleDB' #DSN for ODBC
ORACLESTRING = 'DSN=%s;PWD=%s' % (DBNAME, PWD) #oracle connection string


class ReturnStream:
    def __init__(self,startdates,enddates,returns):
        """
        Constructor for class to hold a time series of returns.
        
        Parameters
        ----------
        startdates : list of datetimes
            startdates of percentage returns
        enddates : list of datetimes
            enddates of percentage returns
        
        """
        self.startdates = startdates
        self.enddates = enddates
        self.setdates = set(enddates)
        self.returns = returns
    
    def std(self):
        """Returns standard deviation of returns."""
        return scipy.std(self.returns)
    
    def changefreq(self, freq='W', dayofweek=2, forcedates=False):
        """
        Changes the frequency of returns.
        
        Parameters
        ----------
        freq : char (default 'W')
            'W' for weekly,
            'M' for monthly
        dayofweek : int (default 2)
            if weekly frequency, force the end
            of the week to be on this day (2==wednesday)
        forcedates : bool (default False)
            if True and weekly frequency is selected,
            then force a return on Wednesday.
        """
        mydates = []
        if freq=='W':
            if forcedates:
                for enddate in self.enddates:
                    if enddate.weekday()==dayofweek:
                        while(enddate<self.enddates[-1]):
                            origend = enddate
                            while not(enddate in self.enddates):
                                enddate = enddate - datetime.timedelta(days=1)
                            mydates.append(enddate)
                            enddate=origend+datetime.timedelta(days=7)
                        return self.datereturns(mydates)
            else:
                for enddate in self.enddates:
                    if enddate.weekday()==dayofweek:
                        mydates.append(enddate)
                return self.datereturns(mydates)
        elif freq=='M':
            for i in range(0,len(self.enddates)):
                if (not(i==len(self.enddates)-1)) and (self.enddates[i].month!=self.enddates[i+1].month):
                    mydates.append(self.enddates[i])
            return self.datereturns(mydates)
                
    
    def __getitem__(self,index):
        """
        Bracket code for the class.
        
        Parameter
        ---------
        index : slice or datetime
            if datetime, get the return on the end date;
            if it's a slice, get all returns between the 
            two dates.
        
        Returns
        -------
        if index is a slice, returns a ReturnStream;
        otherwise returns a float.
        """
        if isinstance(index,slice):
            startdate, enddate = index.start, index.stop
            if startdate in self.startdates:
                startindex = self.startdates.index(startdate)
            elif startdate<self.startdates[0]:
                startindex = self.startdates[0]
            else:
                while not(startdate in self.startdates):
                    startdate = startdate + datetime.timedelta(days=1)
                    if startdate>enddate:
                        return None
                startindex = self.startdates.index(startdate)
            if enddate in self.enddates:
                endindex = self.enddates.index(enddate)
            elif enddate>self.enddates[-1]:
                endindex = len(self.enddates)-1
            else:
                while not(enddate in self.enddates):
                    if enddate<startdate:
                        return None
                    enddate = enddate - datetime.timedelta(days=1)
                endindex = self.enddates.index(enddate)
            return ReturnStream(self.startdates[startindex:endindex],self.enddates[startindex:endindex],
                                self.returns[startindex:endindex])
        else:
            if index in self.enddates: #may be a cleaner approach is to have a hash of dates and returns
                return self.returns[self.enddates.index(index)] 
            else:
                return None

    def datereturns(self, overlapdates):     # Return Stream method
        """Given a set of overlapdates, return a ReturnStream that includes just those dates.
        the idea is to find the overlapdates as a subset of the self.enddates and associate to startdate
        how the accumulation rolls down"""
        accumval = 1.0
        returns = []
        firststartdate = None
        for dte in self.enddates:  #if this list is presorted then we can loop over the indices
            dayreturn = self[dte]+1.0  #the [] operator has been overloaded in return stream class via __getitem__
            if dte in overlapdates:
                if not(firststartdate):
                    firststartdate = self.startdates[self.enddates.index(dte)]
                    accumval = 1.0
                returns.append(accumval*dayreturn-1.0)
                accumval = 1.0
            else:  #roll down the investment with realized return
                accumval *= dayreturn
        return ReturnStream([firststartdate]+overlapdates[:-1], overlapdates, scipy.asarray(returns))
    
    def __add__(self,otherstream):
        """Add two streams' returns together and returns a new stream."""
        overlapdates = self.overlap([otherstream])
        stream1returns = self.datereturns(overlapdates)
        stream2returns = otherstream.datereturns(overlapdates)
        return ReturnStream(stream1returns.startdates, stream2returns.enddates, stream1returns.returns+stream2returns.returns)
    
    def __sub__(self,otherstream):
        """Subtracts two streams and returns a new stream."""
        return self.__add__(otherstream.scale(-1.0))

    def scale(self,factor):
        """Multiplies a stream by a scalar and returns another stream."""
        return ReturnStream(self.startdates, self.enddates, self.returns * factor) 

    def overlap(self,otherstreams):
        """Determines the overlapping dates a stream and a list of other streams."""
        overlapdates = self.setdates
        for otherstream in otherstreams:
            overlapdates = overlapdates & otherstream.setdates #intersection of two sets
        overlaplist = list(overlapdates)
        overlaplist.sort()
        return overlaplist
    
    def plot(self):
        """Plots the stream's returns."""
        pylab.plot(self.enddates, self.returns)
        pylab.show()

class BasicStream(ReturnStream):             #BasicStream extends ReturnStream class
    def __init__(self, dates, quotes):
        """
        Constructor for class to hold actual quotes,set of quote dates and returns implied by the quotes.
        dates,quotes are stored as lists and quotes are stored in scipy.ndarray
        Parameters
        ----------
        dates : list of datetimes
            dates of quotes
        quotes : 1-d array
            holds the quotes of the asset
        """
        self.dates = dates
        self.startdates = self.dates[0:-1]
        self.enddates = self.dates[1:]
        self.setdates = set(self.enddates) #this is a set for later operations in return streams
        self.quotes = quotes
        lenreturns = len(quotes)
        self.returns = scipy.zeros(shape=(lenreturns-1,1))  
        #self.returns = self.quotes[1:]/self.quotes[0:-1]-1.0
        for i in range(0,lenreturns-1):
            self.returns[i] = (self.quotes[i+1]/float(self.quotes[i]))-1 if self.quotes[i] !=0 else 0
           
    
    def dayquote(self,key):
        """Returns the quote for a given done or a small number if that quote doesn't exist."""
        if key in self.dates:
            return self.quotes[self.dates.index(key)]
        else:
            return 1E-7
        
def hybridstream(dates1,dates2,quotes1,quotes2):   #stand alone function
    """
    Creates a new stream that represents the maximum of two other basic streams.
    Currently the function is programmed to handle data related 2 fund streams
    
    Parameters
    ----------
    dates1, dates2 : list of datetimes
        dates for both streams
    quotes1, quotes2 : 1-d array
        quotes for both streams
    """
    st1 = BasicStream(dates1,quotes1) #BASENAV
    st2 = BasicStream(dates2,quotes2) #PNDY
    #overlaplist = list(set([date for date in st1.enddates if date in st2.enddates]))
    overlapdts = st1.overlap([st2])  #overlap method of returnstream class. Common dates of funds date info
    ol1 = st1.datereturns(overlapdts)
    ol2 = st2.datereturns(overlapdts)
    returntest = (ol1.returns>=ol2.returns)
    greatret = returntest*ol1.returns + (1-returntest)*ol2.returns # dysfunctional
    return ReturnStream(ol1.startdates,ol1.enddates,greatret)
        

def getmarketdatadb(connectstring=ORACLESTRING, DBName = 'ODSACT.ACT_RSL_EQTY_PRICE_HIST',
                    ValuationDateField = 'VALUATION_DT'):
    """Grabs the available market data out of the database,
    and puts it into a dictionary of return streams. In case of null values for the field values,they are replaced by 0"""
    cnxn = pyodbc.connect(connectstring)
    c = cnxn.cursor()
    sql = 'SELECT * FROM %s ORDER BY %s;' % (DBName,ValuationDateField)
#     sql = 'SELECT * FROM ODSACT.ACT_RSL_EQTY_PRICE_HIST ORDER BY VALUATION_DT;'
    c.execute(sql)  #sql
    dates, spx, rty, eafe, agg, tbill = [],[],[],[],[],[]
    rows = c.fetchmany(100)
    while rows:
        for row in rows:                                          # for row in c.fetchall()
            dates.append(row.VALUATION_DT)  if row.VALUATION_DT is not None else dates.append(datetime.datetime(1900,1,1))
            if row.VALUATION_DT is None:
                print 'ValuationDate is NULL.Set to Jan-1-1900'
            spx.append(float(row.SPTR))     if row.SPTR         is not None else spx.append(0.0)
            rty.append(float(row.RU20INTR)) if row.RU20INTR     is not None else rty.append(0.0) 
            eafe.append(float(row.GDDUEAFE))if row.GDDUEAFE     is not None else eafe.append(0.0)
            agg.append(float(row.LBUSTRUU)) if row.LBUSTRUU     is not None else agg.append(0.0)
            tbill.append(float(row.CASH))   if row.CASH         is not None else tbill.append(0.0)
        rows = c.fetchmany(100)
    return {  'SPX'     : BasicStream(dates,scipy.asarray(spx)),
              'RTY'     : BasicStream(dates,scipy.asarray(rty)),
              'EAFE'    : BasicStream(dates,scipy.asarray(eafe)),
              'AGG'     : BasicStream(dates,scipy.asarray(agg)),
              'TBILL'   : BasicStream(dates,scipy.asarray(tbill))}

def parsemarketfile(filename):
    """Parses market data out of a file into a dictionary of streams."""
    f = open(filename)
    csvparser = csv.reader(f)
    names,dates,quotes, streambasket = [], {}, {}, {}
    for row in csvparser:
        if len(names)==0:
            indexcount = len(row)/2
            for i in range(0,indexcount):
                indexname = row[2*i+1]
                names.append(indexname)
                dates[indexname] = []
                quotes[indexname] = []
        else:
            for i in range(0,indexcount):
                if row[2*i]:
                    dates[names[i]].append(datetime.datetime.strptime(row[2*i],'%Y%m%d'))
                    quotes[names[i]].append(float(row[2*i+1]))
    for index in names:
        streambasket[index] = BasicStream(dates[index],scipy.asarray(quotes[index]))
    return streambasket
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
            if index in self.enddates:
                return self.returns[self.enddates.index(index)]
            else:
                return None

    def datereturns(self, overlapdates):
        """Given a set of dates, return a ReturnStream that includes just those dates."""
        accumval = 1.0
        returns = []
        firststartdate = None
        for dte in self.enddates:
            dayreturn = self[dte]+1.0
            if dte in overlapdates:
                if not(firststartdate):
                    firststartdate = self.startdates[self.enddates.index(dte)]
                    accumval = 1.0
                returns.append(accumval*dayreturn-1.0)
                accumval = 1.0
            else:
                accumval *= dayreturn
        return ReturnStream([firststartdate]+overlapdates[1:], overlapdates, scipy.asarray(returns))
    
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
            overlapdates = overlapdates & otherstream.setdates
        overlaplist = list(overlapdates)
        overlaplist.sort()
        return overlaplist
    
    def plot(self):
        """Plots the stream's returns."""
        pylab.plot(self.enddates, self.returns)
        pylab.show()

class BasicStream(ReturnStream):
    def __init__(self, dates, quotes):
        """
        Constructor for class to hold actual quotes, which are then converted to percent returns.
        
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
        self.setdates = set(self.enddates)
        self.quotes = quotes
        self.returns = self.quotes[1:]/self.quotes[0:-1]-1.0
    
    def dayquote(self,key):
        """Returns the quote for a given done or a small number if that quote doesn't exist."""
        if key in self.dates:
            return self.quotes[self.dates.index(key)]
        else:
            return 1E-7
        
def hybridstream(dates1,dates2,quotes1,quotes2):
    """
    Creates a new stream that represents the maximum of two other basic streams.
    
    Parameters
    ----------
    dates1, dates2 : list of datetimes
        dates for both streams
    quotes1, quotes2 : 1-d array
        quotes for both streams
    """
    st1 = BasicStream(dates1,quotes1)
    st2 = BasicStream(dates2,quotes2)
    overlapdts = st1.overlap([st2])
    ol1 = st1.datereturns(overlapdts)
    ol2 = st2.datereturns(overlapdts)
    greatret = (ol1.returns>=ol2.returns)*ol1.returns+(ol2.returns>ol1.returns)*ol2.returns
    return ReturnStream(ol1.startdates,ol1.enddates,greatret)
        

def getmarketdatadb(connectstring=ORACLESTRING):
    """Grabs the available market data out of the database,
    and puts it into a dictionary of return streams."""
    cnxn = pyodbc.connect(connectstring)
    c = cnxn.cursor()
    sql = 'SELECT * FROM ODSACT.ACT_RSL_EQTY_PRICE_HIST_OLD ORDER BY VALUATION_DT;'
    c.execute(sql)
    dates, spx, rty, eafe, agg, tbill = [],[],[],[],[],[]
    for row in c.fetchall():
        dates.append(row.VALUATION_DT)
        spx.append(float(row.SPTR))
        rty.append(float(row.RTY))
        eafe.append(float(row.GDDUEAFE))
        agg.append(float(row.LBUSTRUU))
        tbill.append(float(row.CASH))
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
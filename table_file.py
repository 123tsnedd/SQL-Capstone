'''
Created By Trevor Snedden

File contains all tables and columns for the database. can add or remove to keep sqlFile cleaner
'''


def create_telpos():
    return '''
    CREATE TABLE IF NOT EXISTS telpos(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    epoch REAL,
    ra REAL,
    dec REAL,
    el REAL,
    ha REAL,
    am REAL,
    rotoff REAL,
    PRIMARY KEY(ts)
    );
    '''

def create_teldata():
    return '''
    CREATE TABLE IF NOT EXISTS teldata(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    roi REAL,
    tracking REAL,
    guiding REAL,
    slewing REAL,
    guiderMoving REAL,
    az REAL,
    zd REAL,
    pa REAL,
    domeAz REAL,
    domeStat REAL,
    PRIMARY KEY (ts)
    );
    '''

def create_telenv():
    return '''
    CREATE TABLE IF NOT EXISTS telenv(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    tempout REAL,
    pressure REAL,
    humidity REAL,
    wind REAL,
    winddir REAL,
    temptruss REAL,
    tempcell REAL,
    tempseccell REAL,
    tempamb REAL,
    dewpoint REAL,
    PRIMARY KEY (ts)
    );
    '''

def create_telcat():
    return '''
    CREATE TABLE IF NOT EXISTS telcat(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    catObj VARCHAR(8),
    catRm VARCHAR(8),
    catRa REAL,
    catDec REAL,
    catEp REAL,
    catRo REAL,
    PRIMARY KEY (ts)
    );
    '''

def create_telsee():
    return '''
    CREATE TABLE IF NOT EXISTS telsee(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    dimm_time REAL,
    dimm_el REAL,
    dimm_fwhm REAL,
    dimm_fwhm_corr REAL,
    mag1_time REAL,
    mag1_el REAL,
    mag1_fwhm REAL,
    mag1_fwhm_corr REAL,
    mag2_time REAL,
    mag2_el REAL,
    mag2_fwhm REAL,
    mag2_fwhm_corr REAL,
    PRIMARY KEY (ts)
    );
    '''

    #  mag1_fwhm SET POSITION AFTER mag1_el;
    # telpos DROP CONSTRAINT telpos_pkey;
    # teldata DROP CONSTRAINT teldata_pkey;
    # telenv DROP CONSTRAINT telenv_pkey;
    # telcat DROP CONSTRAINT telcat_pkey;
    # telsee DROP CONSTRAINT telsee_pkey;
    #  telvane();

    
def create_telvane():
    return '''
    CREATE TABLE IF NOT EXISTS telvane( 
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(15),
    secz REAL,
    encz REAL,
    secx REAL,
    encx REAL,
    secy REAL,
    ency REAL,
    sech REAL,
    ench REAL,
    secv REAL,
    encv REAL,
    PRIMARY KEY (ts)
    );
    '''

def create_observer():
    return '''
    CREATE TABLE IF NOT EXISTS observer(
    ts TIMESTAMP,
    prio VARCHAR(8),
    ec VARCHAR(20),
    email VARCHAR(40),
    obsName VARCHAR(100),
    observing BOOLEAN,
    PRIMARY KEY (ts)
    );
    '''

def build_all():
    return create_telpos(), create_teldata(), create_telenv(), create_telcat(), create_telsee(), create_telvane(), create_observer()

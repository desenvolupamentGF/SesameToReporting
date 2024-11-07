# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 1
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!

# for logging purposes
import logging

# Import needed library for HTTP requests
import requests

# extra imports
import sys
import datetime
import calendar
from utils import send_email, connectSQLServer, disconnectSQLServer
import os

# FELIX-IMPORTANT - API Sesame at https://apidocs.sesametime.com/    (with region "eu2")
URL_EMPLOYEES_SESAME = "/core/v3/employees"
URL_TIMEENTRIES_SESAME = "/project/v1/time-entries"
SESAME_INSTALLACIO_DEPARTMENT_ID = "c6c30fb6-565b-4109-b370-dc50d93a143b"
SESAME_RESIDENCIAL_DEPARTMENT_ID = "4d2eeee6-b936-446c-8b88-7869f6483a87"
SESAME_POSTVENDA_DEPARTMENT_ID = "9cca5654-3d1b-49f7-b3db-728ef40f9b43"
URL_API_SESAME = os.environ['URL_API_SESAME']
TOKEN_API_SESAME = os.environ['TOKEN_API_SESAME']

# Teowin Database constants
TEOWIN_SQLSERVER_USER = os.environ['TEOWIN_SQLSERVER_USER']
TEOWIN_SQLSERVER_PASSWORD = os.environ['TEOWIN_SQLSERVER_PASSWORD']
TEOWIN_SQLSERVER_HOST = os.environ['TEOWIN_SQLSERVER_HOST']
#TEOWIN_SQLSERVER_HOST = os.environ['TEOWIN_SQLSERVER_HOST']
TEOWIN_SQLSERVER_DATABASE = os.environ['TEOWIN_SQLSERVER_DATABASE']

# Biostar Database constants
BIOSTAR_SQLSERVER_USER = os.environ['BIOSTAR_SQLSERVER_USER']
BIOSTAR_SQLSERVER_PASSWORD = os.environ['BIOSTAR_SQLSERVER_PASSWORD']
BIOSTAR_SQLSERVER_HOST = os.environ['BIOSTAR_SQLSERVER_HOST']
#BIOSTAR_SQLSERVER_HOST = os.environ['BIOSTAR_SQLSERVER_HOST']
BIOSTAR_SQLSERVER_DATABASE = os.environ['BIOSTAR_SQLSERVER_DATABASE']

# Other constants
CONN_TIMEOUT = 50
DAYS_TO_RECALCULATE = 50

def synchronize_timeentries(dbTeowin, myCursorTeowin, dbBiostar, myCursorBiostar, now, strFrom, strTo, prefixTeowin, prefixBiostar, department):

    # processing timeentries from origin ERP (Sesame)
    try:
        i = 0
        j = 0
        endProcess1 = False
        page1 = 1        
        while not endProcess1:

            headers = {
                "Authorization": "Bearer " + TOKEN_API_SESAME, 
                "Content-Type": "application/json"
            }

            get_req1 = requests.get(URL_API_SESAME + URL_EMPLOYEES_SESAME + "?page=" + str(page1) + "&departmentIds=" + str(department) + "&status=active", 
                                   headers=headers, verify=False, timeout=CONN_TIMEOUT)
            response1 = get_req1.json()

            for data1 in response1["data"]:

                i = i + 1

                idEmployee = data1["id"]
                firstName = data1["firstName"]
                lastName = data1["lastName"]
                codEmployee = data1["customFields"][0]["value"]

                if str(codEmployee) == "":
                    logging.error('      Employee without code, check why. Name: ' + str(firstName) + ' ' + str(lastName))                
                    continue # Next!                    

                logging.info('      Processing employee code: ' + str(codEmployee) + ', name: ' + str(firstName) + ' ' + str(lastName))

                strFromAux = strFrom
                while (strFromAux <= strTo):

                    endProcess2 = False
                    page2 = 1        
                    while not endProcess2:

                        get_req2 = requests.get(URL_API_SESAME + URL_TIMEENTRIES_SESAME + "?page=" + str(page2) + "&employeeId=" + str(idEmployee) + "&from=" + str(strFromAux) + "&to=" + str(strFromAux) + "&employeeStatus=active", 
                                               headers=headers, verify=False, timeout=CONN_TIMEOUT)
                        response2 = get_req2.json()

                        numOT = 0
                        for data2 in response2["data"]:

                            if data2["project"] == None:
                                logging.warning('         Timeentry sense projecte per ' + str(firstName) + ' ' + str(lastName) + ' i data ' + str(strFromAux) + '. No la processem ...')
                                continue # Not used. Next!
                            try:
                                numOT = int(data2["project"]["name"][2:7])
                            except Exception as e:
                                logging.warning('         Timeentry sense OT correcte per ' + str(firstName) + ' ' + str(lastName) + ' i data ' + str(strFromAux) + '. No la processem ...')
                                continue # Not used. Next!
                            numOF = "INSTAL·LADOR"
                            seccion = "S90"
                            if data2["timeEntryOut"] == None:
                                logging.warning('         Timeentry sense sortida assignada per ' + str(firstName) + ' ' + str(lastName) + ' i data ' + str(strFromAux) + '. No la processem ...')                                
                                continue # Not used. Next!
                            timeEntryOut = data2["timeEntryOut"]["date"]
                            if data2["timeEntryIn"] == None:
                                logging.warning('         Timeentry sense entrada assignada per ' + str(firstName) + ' ' + str(lastName) + ' i data ' + str(strFromAux) + '. No la processem ...')                                
                                continue # Not used. Next!                            
                            timeEntryIn = data2["timeEntryIn"]["date"]

                            timeOut = datetime.datetime.fromisoformat(timeEntryOut)
                            timeIn = datetime.datetime.fromisoformat(timeEntryIn)
                            numSeconds = (timeOut - timeIn).total_seconds()
                            numMinutes = int(numSeconds / 60)

                            myCursorTeowin.execute("SELECT MAX(codObra) " \
                                                   "FROM " + prefixTeowin + "tObras " \
                                                   "WHERE OT = '" + str(numOT) + "' ")
                            record = myCursorTeowin.fetchone()   
            
                            if record is None:           
                                logging.warning('         OT no trobada (' + str(numOT) + ') per ' + str(firstName) + ' ' + str(lastName) + ' i data ' + str(strFromAux) + '. No la processem ...')                                
                            else:
                                codObra = str(record[0]).strip()

                                anyo = strFromAux.year
                                mes = strFromAux.month
                                if len(str(mes)) == 1:
                                    mes = '0' + str(mes)
                                dia = strFromAux.day
                                if len(str(dia)) == 1:
                                    dia = '0' + str(dia)

                                # FELIX IMPORTANT: My inserts are all with CodPresupuesto = 'INSTAL·LADOR'.
                                sql = "INSERT INTO " + prefixTeowin + "tobras_horasfabricacion (CodObra, CodEmpleado, CodSeccion, CodPresupuesto, Minutos, Mes, Anyo, Dia) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                                val = (str(codObra), str(codEmployee), str(seccion), str(numOF), str(numMinutes), str(mes), str(anyo), str(dia))
                                myCursorTeowin.execute(sql, val)
                                dbTeowin.commit()    

                                try: # FELIX IMPORTANT: My inserts are all with absenciaTipus = '-'.
                                    t = datetime.datetime(int(anyo), int(mes), int(dia), 0, 0, 0)
                                    epoch = calendar.timegm(t.timetuple())

                                    sql = "INSERT INTO " + prefixBiostar + "xip_presencia (dia, nUserID, hEnt, hSort, hTreballades, hDescans, cEntSort, cDescans, absenciaTipus, absenciaHores, info, hExtres, hDia, hDpte, xipMarcatNuvol, hTreballadesNormals, hTreballadesNocturnes, hExtresNormals, hExtresNocturnes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                    val = (str(epoch), str(codEmployee), '0', '0', '0', '0', '', '', '-', '0', '', '0', '480', '0', '0', '0', '0', '0', '0')
                                    myCursorBiostar.execute(sql, val)
                                    dbBiostar.commit()    
                                except Exception as e: # No problem if already exists (PK is dia plus nUserID)
                                    None

                                j = j + 1

                        meta2 = response2["meta"]
                        if str(meta2["lastPage"]) == str(page2):
                            endProcess2 = True
                        else:
                            page2 = page2 + 1

                    strFromAux = strFromAux + datetime.timedelta(1)

            meta1 = response1["meta"]
            if str(meta1["lastPage"]) == str(page1):
                endProcess1 = True
            else:
                page1 = page1 + 1

        logging.info('      Total processed workers: ' + str(i) + '. Total records added into final table: ' + str(j) + '. ')           

    except Exception as e:
        logging.error('   Unexpected error when processing timeentries from original ERP (Sesame): ' + str(e))
        send_email("SesameToReporting", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbTeowin)
        disconnectSQLServer(dbBiostar)
        sys.exit(1)

def main():

    executionResult = "OK"

    if ENVIRONMENT == 1:
        prefixTeowin = "GF3D.dbo."
        prefixBiostar = "[BIO\BSSERVER].[BioStar].[dbo]."
    else:
        prefixTeowin = "GF3DTEST2.dbo."
        prefixBiostar = "[BIO\BSSERVER].[BioStarTest2].[dbo]."

    # current date and time
    now = datetime.datetime.now() 

    # Period to calculate (normally from last 50 days to yesterday)
    strFrom = datetime.date.today() - datetime.timedelta(DAYS_TO_RECALCULATE)
    strTo = datetime.date.today() - datetime.timedelta(1)

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE_SesameToReporting'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START export Sésame to Reporting - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to Teowin database (SQLServer)
    dbTeowin = None
    try:
        dbTeowin = connectSQLServer(TEOWIN_SQLSERVER_USER, TEOWIN_SQLSERVER_PASSWORD, TEOWIN_SQLSERVER_HOST, TEOWIN_SQLSERVER_DATABASE)
        myCursorTeowin = dbTeowin.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to SQLServer Teowin database: ' + str(e))
        send_email("SesameToReporting", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbTeowin)
        sys.exit(1)

    # connecting to Biostar database (SQLServer)
    dbBiostar = None
    try:
        dbBiostar = connectSQLServer(BIOSTAR_SQLSERVER_USER, BIOSTAR_SQLSERVER_PASSWORD, BIOSTAR_SQLSERVER_HOST, BIOSTAR_SQLSERVER_DATABASE)
        myCursorBiostar = dbBiostar.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to SQLServer Biostar database: ' + str(e))
        send_email("SesameToReporting", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectSQLServer(dbBiostar)
        sys.exit(1)

    # deleting rows to be re-calculated later again
    strFromAux = strFrom
    while (strFromAux <= strTo):
        anyo = strFromAux.year
        mes = strFromAux.month
        if len(str(mes)) == 1:
            mes = '0' + str(mes)
        dia = strFromAux.day
        if len(str(dia)) == 1:
           dia = '0' + str(dia)

        myCursorTeowin.execute("DELETE FROM " + prefixTeowin + "tobras_horasfabricacion WHERE codSeccion = 'S90' AND CodPresupuesto = 'INSTAL·LADOR' AND Mes = '" + str(mes) + "' AND Anyo = '" + str(anyo) + "' AND dia = '" + str(dia) + "' ")
        dbTeowin.commit()

        strFromAux = strFromAux + datetime.timedelta(1)

    logging.info('   Processing timeentries from origin ERP (Sesame) - INSTAL.LADORS')
    synchronize_timeentries(dbTeowin, myCursorTeowin, dbBiostar, myCursorBiostar, now, strFrom, strTo, prefixTeowin, prefixBiostar, SESAME_INSTALLACIO_DEPARTMENT_ID)    
    #logging.info('   Processing timeentries from origin ERP (Sesame) - RESIDENCIAL')
    #synchronize_timeentries(dbTeowin, myCursorTeowin, dbBiostar, myCursorBiostar, now, strFrom, strTo, prefixTeowin, prefixBiostar, SESAME_RESIDENCIAL_DEPARTMENT_ID)    
    #logging.info('   Processing timeentries from origin ERP (Sesame) - POSTVENDA')
    #synchronize_timeentries(dbTeowin, myCursorTeowin, dbBiostar, myCursorBiostar, now, strFrom, strTo, prefixTeowin, prefixBiostar, SESAME_POSTVENDA_DEPARTMENT_ID)    

    # Send email with execution summary
    send_email("SesameToReporting", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END export Sésame to Reporting - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing databases
    dbTeowin.close()
    myCursorTeowin.close()
    dbBiostar.close()
    myCursorBiostar.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()
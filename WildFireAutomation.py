from bs4 import BeautifulSoup
import bs4 as bs
import requests
import csv
import cx_Oracle
import codecs
from contextlib import closing
import time
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Make sure to add these to the requirement.txt

# MACROS -- URLS of the zip files needed.
NASA_BASE = "https://firms.modaps.eosdis.nasa.gov"
NASA_SITE = "https://firms.modaps.eosdis.nasa.gov/active_fire/#firms-txt"
NASA_SITE_SHPE = "https://firms.modaps.eosdis.nasa.gov/active_fire/#firms-shapefile"
zipfile_url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/shapes/zips/MODIS_C6_Global_24h.zip" #this is the actual file comment this out later

MODIS_LINK = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_USA_contiguous_and_Hawaii_24h.csv"
GOOGLE_LINK = "https://www.google.com/"
VIIRS_LINK = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/VNP14IMGTDL_NRT_USA_contiguous_and_Hawaii_24h.csv"

TIME = "24h"
REGION = "USA"

SMTP_SERVER = "smtp.chevron.net"
SENDER = "wildfirebot@chevron.com"
DESTINATION = "davidgan@chevron.com"

ERROR_MSG_SIMPLE = ""
ERROR_MSG_TERMINAL =""
ERROR_NON_CRITICAL = ""
NO_ERROR = "no errors -- updated database"


# Needed to escape the special charachter '@' in the password by using this
passFix = requests.utils.quote("password") # replaced systemID's real password with password
proxy = {
    'http': 'http://user:password@IPAdress'
}


"""
Function that takes an inital url (baseURL), the website you are trying to scrape for the zips (scrapingURL), 
and a set of Proxies and outputs the url of the zipfile you want to extract from as a string.

Slightly overkill since we know the URL of the zips; however, this is ensures we get the right link
if the URLs were to change in any way

In this case:
    baseURL != scrapingURL however that might not always be the case
"""
def find_file_urls(baseURL, scrapingURL ,setOfProxies):
    global ERROR_NON_CRITICAL
    global ERROR_MSG_SIMPLE
    global ERROR_MSG_TERMINAL

    linksList = []
    url = ""
    ext = ""
    # print("pre source")
    try:
        source = requests.get(scrapingURL, proxies = setOfProxies,  stream=True, verify=False)
    except Exception as e:
        ERROR_MSG_SIMPLE = "Error: Bot could not connect to the Nasa Website. Check that the website is up and there are no issues with your connection/proxy.\n"
        ERROR_MSG_TERMINAL = e
        raise Exception
        return []
    #print("pre soup")
    # The soup is just the HTML after using the html parser
    soup = bs.BeautifulSoup(source.content, 'html.parser')

    # This implementation will have to change on a different site. 
    # Look through the html and find all the links that start with the <a> tag
    for link in soup.find(id="mliContent_csv").find_all('a'):

        # Find the links that have 24h(time) and USA(region) in them
        if TIME in link.get('href') and REGION in link.get('href'):

            # Add the base with the extension to get the URL of the csv file
            ext = link.get('href')
            #error handling
            if ext == None:
                ERROR_MSG_SIMPLE = "Error: Could not get the right extension to the satellite links. Check ext in find_file_urls method.\n"
                raise TypeError
            url = baseURL + ext

            # Add the URLs to a list
            linksList.append(url)
            url = "" # clear url for the next one

    # Error Handling -- make sure the list of links is not empty
    if len(linksList) == 0:
        ERROR_MSG_SIMPLE = "Error: No links were added to the linksList -- check find_file_urls method."
        raise Exception

    #print("ERROR within: ", ERROR)
    return linksList


"""
Function that takes as input a list of links and adds all the fire data points that meet a certian
FRP, Confidence level, and are within range of polygons to the database. Returns no output.
"""
def save_file_to_DB(links):
    global ERROR_NON_CRITICAL
    global ERROR_MSG_SIMPLE
    global ERROR_MSG_TERMINAL
    # Boolean flag to keep track 
    isModis = 0

    # Create a connection with the Oracle Database.
    try:
        con = cx_Oracle.connect('NOAA_APP_USER/password')
    except Exception as e:
        # Connection Error connecting to Oracle Database
        ERROR_MSG_SIMPLE = "Error: Could not connect to oracle database -- check save_file_to_DB method."
        ERROR_MSG_TERMINAL = e
        return
        #print("messed up oracle connection")

    else:
        # Create a cursor obect and call its execute method to perform SQL commands.
        cur = con.cursor()

    # Iterate through the different satellite csvs
    for i in range(len(links)):
        url = links[i]

        # Identify which satellite data we are looking at.
        if "MODIS" in url:
            isModis = 1
        elif "viirs" in url:
            isModis = 0
        else:
            # Handle if we possibly grabbed a link that wasn't to MODIS or VIIRS satelite. Not a critical error.
            ERROR_NON_CRITICAL += "Grabbed a file that was neither MODIS or VIIRS -- check save_file_to_DB method <br><br>"
            
        
        # Fix bug in Oracle so you can use unicode strings for a timestamp parameter
        # https://stackoverflow.com/questions/15396241/cx-oracle-ora-01843-not-a-valid-month-with-unicode-parameter
        cur.execute(
        "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")

        # Access the file and read it into the database.
        with closing(requests.get(url, proxies=proxy, stream=True)) as r:
            csv_reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')

            # Skip the column headers row.
            count = 0
            next(csv_reader)
            for lines in csv_reader:
                if isModis:
                    try:
                        if float(lines[11]) > 10 and float(lines[8]) >= 75:
                            cur.execute("insert into cpl_comm.firms_active_fires (objectid, x_coord, y_coord, instrument, shape, brightness, scan, track, acq_date, acq_time, satellite, confidence, version, bright_T31, frp, daynight, load_date) values (sde.gdb_util.next_rowid('CPL_COMM','FIRMS_ACTIVE_FIRES'), {1}, {0}, 'MODIS', sde.st_pointfromtext('point ({1} {0})', 4326), {2}, {3}, {4}, '{5}', {6}, '{7}', {8}, '{9}', {10}, {11}, '{12}', TO_DATE('{13}', 'dd-MON-yyyy hh24:mi:ss'))"
                                .format(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8], lines[9], lines[10], lines[11], lines[12], datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")))
                    except:
                        ERROR_NON_CRITICAL += "****** ERROR ADDING A MODIS ROW ********"
                    
                else: # if the satellite is VIIRS
                    try:
                        if float(lines[11]) > 10 and (lines[8] == "nominal" or lines[8] == "high"):
                            cur.execute("insert into cpl_comm.firms_active_fires (objectid, x_coord, y_coord, instrument, shape, bright_TI4, scan, track, acq_date, acq_time, satellite, confidence_text, version ,bright_TI5, frp, daynight, load_date) values (sde.gdb_util.next_rowid('CPL_COMM','FIRMS_ACTIVE_FIRES'), {1}, {0}, 'VIIRS', sde.st_pointfromtext('point ({1} {0})', 4326), {2}, {3}, {4}, '{5}', {6}, '{7}', '{8}', '{9}', {10}, {11}, '{12}', TO_DATE('{13}', 'dd-MON-yyyy hh24:mi:ss'))"
                                .format(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8], lines[9], lines[10], lines[11], lines[12], datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")))
                    except:
                        #print("****** ERROR ADDING A VIIRS ROW *******")
                        ERROR_NON_CRITICAL += "****** ERROR ADDING A VIIRS ROW ********"

    #print("finished adding to database")
    cur.close() # Close the cursor now.
    con.commit() # Commit pending transactions to the database.
    con.close() # Close the connection now.
    return

"""
Method that takes as input an error message and sends an email from SENDER to DESTINATION
and if there are no errors found it will send all potentially bad fires and if there is 
an error it will send the error message.
"""
def sendEmail(noProbs, emailbody):
    
    # Setting up the correct format for the email (subject line, body, etc..)
    msg = MIMEMultipart()

    # Set From account and To account
    msg['From'] = "WildfireBot@Chevron.com"
    msg['To'] = DESTINATION

    # Build Email object
    if  noProbs == 1: # no problems found
        # Send a successful email.
        msg['Subject'] = "WILDFIRE BOT RAN"
        body = "<p>DO NOT REPLY TO THIS EMAIL <br><br>" + "The bot ran successfully...<br><br>" + emailbody #"We found a dangerous wildfire <a href=""http://cplcmap-dev.chevron.com/cmap/Index.html?viewer=CMap#"">hereeeeeeeee</a></p>"
        msg.attach(MIMEText(body, 'html'))

    if noProbs == 0: # any sort of problem found
        # Send a faulted email.
        msg['Subject'] = "WILDFIRE BOT FAULTED"
        body = "<p>DO NOT REPLY TO THIS EMAIL <br><br>" + "The bot encountered an error...<br><br>" + emailbody + "</p>"
        msg.attach(MIMEText(body, 'html'))

    # Create an SMTP instance the encapuslates and SMTP connection
    # https://docs.python.org/3/library/smtplib.html#smtplib.SMTPConnectError
    server = smtplib.SMTP(SMTP_SERVER) 

    # Put the SMTP connection in TLS (Transport Layer Security) mode. So all SMTP commands following will be encrypted
    server.starttls()
    server.ehlo() #identifies self to server

    # Send the email.
    server.sendmail(msg['From'], msg['To'], msg.as_string())

    #Close the server
    server.quit()
    return


"""
Main method that runs the script by calling all the helper functions in the correct order.
"""
def main():
    global ERROR_NON_CRITICAL
    global ERROR_MSG_SIMPLE
    global ERROR_MSG_TERMINAL

    # 1 is true so if noError = 1 then there are no critical errors to worry about :) and we send the bot ran successfully email
    noError = 1 

    # Build a return variable (string) that will be the text shown in the log of each UIPath run.
    logMessage = "No critical errors found -- Added new data to database unless there were ERRORS adding rows below..."
    print("Starting Process at: ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Parse the website HTML to find the links to the csv files and add them to a list
    try:
        linksList = find_file_urls(NASA_BASE, NASA_SITE, proxy)
    except:
        # There was a critical error in the find_file_urls method (Could not make a GET request on the ), so build and send a bot faulted email and stop main by returning.
        # Build the email body.
        logMessage = "Error: Could not get the links to the csvs -- problem within find_file_urls method.<br><br>" + str(ERROR_MSG_SIMPLE) + "<br><br>" + "Terminal Error Message: " + str(ERROR_MSG_TERMINAL) + "<br><br>" + "Other errors found but not necessary of stopping encountered: "  + str(ERROR_NON_CRITICAL)
        noError = 0
        # Send the bot faulted email.
        sendEmail(noError, logMessage)
        return logMessage

    # Go through the list of links and add the data to the Oracle Database.
    try:
        save_file_to_DB(linksList)
    except: 
        # There was a critical error in the save_file_to_DB method (Could not connect to Oracle Database), so build and send a bot faulted email and stop main by returning.
        # Build the email body.
        logMessage = "Error: could not process the links and store them in the oracle DB save_file_to_DB in main.<br><br> " + str(ERROR_MSG_SIMPLE) + "<br><br>" + "Terminal Error Message: " + str(ERROR_MSG_TERMINAL) + "<br><br>" + "Other errors not necessary of stopping encountered: "  + str(ERROR_NON_CRITICAL)
        noError = 0
        # Send the bot faulted email.
        sendEmail(noError, logMessage)
        return logMessage
    
    # Will only get here if there were no critical errrors (Critical errors include: Not being able to access the website or not being able to connect to the database)
    logMessage = logMessage +  "<br><br>" + ERROR_NON_CRITICAL
    # Send a bot ran successfully 
    sendEmail(noError, logMessage) ## change one to no error
    print("Finished Process at: ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return logMessage

main()

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import hero.util
import validators
import couchdb
import gspread
import time
import datetime
import requests
import re
import httplib
import urlparse
import sys
# ugly workarounds
import webbrowser


####################
# initialize config items

# web handling definitions
url_header = 'https://docs.google.com/spreadsheets/d/'
http_header = {'user-agent': 'hero/1.0'}

# utility definitions
# TODO: move this to a metadata DB
# what do people write that means empty/no-value
blank_entries = [
    None,
    '',
    '-',
    '--',
    '---',
    'NA',
    'na',
    'N/A',
    'N/a',
    'n/a',
    'None',
    'none',
]
# which Last Revised dates are known to be Template v2.x revision dates
known_v2_revisions = [
    'Last Revised 2015',
    'Last Revised 2016',
    'Last Revised 2017-01-05',
]
# known Associations
# TODO: when does JRDA become a viable association?
known_associations = [
    'WFTDA',
    'MRDA',
    'Other',
]
game_types = [
    'Champs',
    'Playoff',
    'Sanc',
    'Reg',
    'Other',
]
ref_roles = [
    'THR',
    'CHR',
    'HR',
    'IPR',
    'JR',
    'OPR',
    'ALTR',
]
nso_roles = [
    'THNSO',
    'CHNSO',
    'HNSO',
    'PT',
    'PW',
    'IWB',
    'OWB',
    'JT',
    'SO',
    'SK',
    'PBM',
    'PBT',
    'LT'
    'ALTN'
]
known_roles = ref_roles + nso_roles
known_roles2 = dict()
known_roles2['JR'] = {'order': 'Referee', 'family': 'JR', 'name': 'Jammer Referee', 'active': 'true'}
known_roles2['CHNSO'] = {'order': 'NSO', 'family': 'HNSO', 'name': 'Crew Head NSO', 'active': 'true'}
known_roles2['OWB'] = {'order': 'NSO', 'family': 'PT', 'name': 'Outside Whiteboard'}
# TODO handle NSO families somehow - might be better places in query rather than storage
# nso_family = dict()
# nso_family['ch'] = ['CHNSO']
# nso_family['pt'] = ['PT', 'PW', 'IWB', 'OWB']
# nso_family['st'] = ['JT', 'SO', 'SK']
# nso_family['pm'] = ['PBM', 'PBT', 'LT']
# Endorsement section
endorsements = dict()
endorsements['HR'] = 'HR'
endorsements['Head Ref'] = 'HR'
endorsements['IPR'] = 'IPR'
endorsements['Inside Pack'] = 'IPR'
endorsements['JR'] = 'JR'
endorsements['Jam'] = 'JR'
endorsements['OPR'] = 'OPR'
endorsements['Outside Pack'] = 'OPR'
endorsements['HNSO'] = 'Head NSO'
endorsements['Head NSO'] = 'Head NSO'
endorsements['ST'] = 'Score & Timing'
endorsements['Scor'] = 'Score & Timing'
endorsements['PM'] = 'Penalty Management'
endorsements['Penalty Management'] = 'Penalty Management'
endorsements['PT'] = 'Penalty Tracking'
endorsements['Tracking'] = 'Penalty Tracking'
endorsements['MRDA'] = 'MRDA Recognized'

####################
# configure stale_time to determine if the history doc needs to be reloaded
SECONDS = 1
MINUTES = 60*SECONDS
HOURS = 60*MINUTES
DAYS = 24*HOURS
too_soon_to_retry_failures = time.time() - 2*HOURS
# too_soon_to_retry_failures = time.time() # short (debug) time
stale_time = time.time() - 30*DAYS
# stale_time = time.time() - 2*DAYS # short (debug) time

# max run time before timing out:
max_run_time = 15*MINUTES
####################


####################
# Initialize datastores
# couch_server = 'http://hero:oreh@themcclure.synology.me:59841' # docker image on the great machine
couch_server = 'http://hero:oreh@heroic.databutler.ca:59841' # docker image on the great machine with a hopefully more reliable DYNDNS
# couch_server = 'https://hero:oreh@couchdb-f40e3a.smileupps.com/' # hosted
local_couch_server = 'http://admin:nimda@127.0.0.1:5984' # replicated local image'
couch_server = local_couch_server
couch = couchdb.Server(couch_server)
# couch = couchdb.Server(local_couch_server)
offdb = couch['hero']
faileddb = couch['heroic_failures']
# TODO: add in officials-aliases db to track alternative names for finding them later
# TODO: add in metadata db to provide lookups
####################


####################
# Initialize authentication with Google
def init_google_connection():
    """
    Authenticate end user from file first, if the credentials are valid
    NOTE: Flows don't seem to return a refresh token, so when the access
    token expires, you have to invoke user interaction
    :return: authorized connection object
    """
    # shared secrets
    CLIENT_ID = '48539282111-07fidfl1225gaiqk49ubb6r1fr21npln.apps.googleusercontent.com'
    CLIENT_SECRET = 'CQ6-3PPwUjB6nZeYujAuqcWo'

    # Set scope of permissions to accessing spreadsheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://docs.google.com/feeds',
             # 'https://sheets.googleapis.com',
             # 'https://drive.googleapis.com',
             # 'https://www.googleapis.com/auth/spreadsheets',
             # 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
             ]
    redirect_uri = 'http://localhost:8080'

    # Initialize the "flow" of the Google OAuth2.0 end user credentials
    flow = OAuth2WebServerFlow(client_id=CLIENT_ID,
                               client_secret=CLIENT_SECRET,
                               scope=scope,
                               redirect_uri=redirect_uri)

    # store the credential, so you don't keep bugging the user
    storage = Storage('creds.data')

    credentials = storage.get()
    # refresh credentials if they're not valid
    if not credentials or credentials.invalid or credentials.access_token_expired:
        credentials = run_flow(flow, storage)

    # if the token is valid but will expire before the max_run_time allowed, then refresh them
    token_expires = (credentials.token_expiry - datetime.datetime.now()).seconds
    print u"Token valid - expires in {} seconds.".format(token_expires)
    if token_expires < max_run_time*2:
        credentials = run_flow(flow, storage)

    # Return an authenticated connection to the Google Sheets API
    return gspread.authorize(credentials)
####################


####################
# Utility functions (to be moved out to another file)
####################
def unshorten_url(url):
    """
    Takes a URL and unshortens it so it can be used by the Google API.
    The URL is opened in a web stream, and returns the eventual destination URL
    :param url: the URL (short or otherwise)
    :return: the destination URL, or None if it's an invalid URL
    """

    # TODO: refactor as a "get document" function that unshortens and iterates through things to get to a doc in the end
    # STEP 0 - if the URL is already a google sheets format, then stop here
    if url_header in url:
        return url

    # STEP1 - strip out officiating history docs that are housed by a google apps domain - thanks Belfast Roller Derby and Jean-Quad Grand Slam
    pattern = u'(https://docs.google.com/a/)(.+/)(spreadsheets/d/.+$)'
    parts = re.match(pattern,url)
    if parts:
        parts = parts.groups()
        url = u'https://docs.google.com/' + parts[2]

    # STEP2 - if this is a HTML view (because they're fuckers who hate life), then try to get the spreadsheet view
    # search for any html flag (so far htmlpub and htmlview) and return the meat of the url with the edit ending instead
    parts = re.search('(.+)/\w*html\w*$', url)
    if parts:
        # get rid of the html part of the url and try to force the edit view
        url = parts.groups()[0] + '/edit'
        # get rid of the weird folders in the URL in between the /spreadsheets/ and the /d/
        parts = re.search('(.+spreadsheets).+(/d.+)', url)
        if parts:
            parts = parts.groups()
            url = parts[0] + parts[1]

    # STEP3 - quick and safe, high level processing of the URL
    try:
        new_url = requests.head(url, headers=http_header, allow_redirects=True).url
    except requests.ConnectionError as e:
        return "error {}".format(str(e))
    if url_header in new_url:
        return new_url

    # STEP4 - if the first pass doesn't give us a spreadsheet, try this lower level approach:
    parsed = urlparse.urlparse(new_url)
    h = httplib.HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    try:
        response = h.getresponse()
    except:
        return new_url
    if response.status / 100 == 3 and response.getheader('Location'):
        if url_header in response.getheader('Location'):
            return response.getheader('Location')
        # last_url = new_url
        # new_url = response.getheader('Location')
        # while  last_url != new_url:
        #     last_url = new_url
        #     new_url = unshorten_url(new_url)
        # return new_url
        else:
            return requests.get(url, headers=http_header).url
    else:
        return requests.get(url, headers=http_header).url
        # return url

####################


####################
# All the support functions (to be moved to Google Sheets specific files)
####################
def get_version(sheet):
    """
    Check the info tabs and make a determination about which version of the officiating history document is being used.
    Different versions keep information in different places
    :param sheet: the loaded workbook
    :return: integer with the version number, or None for unknown version
    """
    sheet_list = map(lambda x: x.title, sheet.worksheets())
    if 'Summary' in sheet_list:
        if ('WFTDA Referee' in sheet_list) or ('WFTDA NSO' in sheet_list):
            # this is an old history doc but it's been modified to change the WFTDA Summary tab name
            return None
        elif 'Instructions' not in sheet_list:
            # this is a new history doc it's been modified to delete the instructions tab (a no no)
            return None
        # this is an edge case I found a couple of times in the excel export, it might not happen hitting the sheet directly
        # elif workbook['Instructions']['A1'].value == 'Loading...':
        #    # found one instance where the Instructions tab was showing "loading" - at the moment this will only happen on the new sheets
        #    return 2
        elif any(map(lambda x: x in sheet.worksheet('Instructions').acell('A104').value, known_v2_revisions)):
            return 2
        else:
            return None
    elif 'WFTDA Summary' in sheet_list:
        return 1
    else:
        return None


def get_name(sheet):
    """
    Picks through the fields looking for a name in preference order of:
        - derby name
        - real name
        - document title
    :param sheet: the connected Google Sheet
    :return: a list of strings with best guess at name, followed by the raw value of each
    """
    summary = sheet.worksheet("Summary")
    dname = get_value(summary.acell('C4').value)
    rname = get_value(summary.acell('C3').value)
    title = get_value(sheet.title)
    # if the name is blank, fall back to real name
    name = dname
    if dname is None:
        name = rname
    # if the name is blank, fall back to doc title
    if name is None:
        name = title
    return [name, dname, rname, title]


def normalize_cert_endorsements(refcert_string, nsocert_string):
    """
    Takes the string from the cert endorsement cells and returns a list of standard endorsements, plus the strings that
    were not recognized
    :param refcert_string: the raw ref string from the endorsement cells
    :param nsocert_string: the raw NSO string from the endorsement cells
    :return: sorted list of recognized endorsements, plus a split of the remaining entries
    """
    # If there's nothing in the string, return None
    # cert_string = ""
    # if refcert_string:
    #     cert_string += refcert_string
    # if nsocert_string:
    #     cert_string += nsocert_string
    # if not cert_string:
    #     return None
    #
    # # at the moment, just return a list of the raw strings
    # return map(lambda x: get_value(x), cert_string.split())
    endorsement_list = list()
    cert_string = ''
    if refcert_string:
        cert_string += refcert_string + ' '
    if nsocert_string:
        cert_string += nsocert_string
    for key in endorsements:
        if key in cert_string:
            endorsement_list.append(endorsements[key])

    # dedupe the list
    endorsement_list = sorted(list(set(endorsement_list)))

    return endorsement_list


def normalize_cert_value(cert_string):
    """
    Takes the cert string from the history, which is a freeform field, and normalizes it to 1-5 or a blank for uncertified.
    Since certification is likely to be something different than 1-5, if there isn't an identifiable number in the cell
    return the contents of the cell. Once the results are seen, they can be normalized too
    :param cert_string: string taken directly from the history sheet
    :return: None, or 1-5, or a string literal of what they have in the cell
    """
    # If there's nothing in the cell, return None
    cert_string = get_value(cert_string)
    if cert_string is None:
        return None
    # if it's already a number, return an int (if it's < 1 or greater than 5, return None)
    elif isinstance(cert_string, float) or isinstance(cert_string, int):
        if (cert_string < 1) or (cert_string > 5):
            return None
        else:
            return int(cert_string)
    # if it's a string with numbers in it, return the first one
    numbers = re.findall(r'\d+', cert_string)
    if numbers:
        return int(numbers[0])
    else:
        # there are no numbers in the cell, look for someone spelling out the numbers:
        if cert_string.upper() == 'ONE':
            return 1
        elif cert_string.upper() == 'TWO':
            return 2
        elif cert_string.upper() == 'THREE':
            return 3
        elif cert_string.upper() == 'FOUR':
            return 4
        elif cert_string.upper() == 'FIVE':
            return 5
        else:
            # there are no valid numbers in the string, so return the string
            return cert_string


def get_value(value, datatype=None, enum=None):
    """
    Takes a string, and returns the value, or None if the content is equivaluent to the "empty string".
    If there is an enum list supplied, it will also only return elements found in that list.
    If a datatype is supplied, there will also be an attempt to see if the input can be matched to that datatype
    :param value: raw spreadsheet value
    :param datatype: if the datatype is listed, the datatype is enforced on return value
    :param enum: a list containing valid values
    :return: interpreted value, as a string (by default) or datatype if listed
    """

    # get rid of trailing/leading spaces
    value = value.strip()

    # if the passed in value is blank, return None
    if value in blank_entries:
        return None

    # if the enum is specified, return an entry in the list, or else None
    if enum:
        if value not in enum:
            return None

    # if the datatype is specified, return an entry in the that format, or else None
    if datatype == 'date':
        # normalize the date separators:
        value = re.sub('[/.:]', '-', value)

        # this will return a datetime object, if it's a valid ISO date format:
        try:
            if datetime.datetime.strptime(value,'%Y-%m-%d'):
                return value
        except:
            pass
        # this will return a datetime object, if it's incorrectly in the US date format:
        try:
            if datetime.datetime.strptime(value, '%m-%d-%Y'):
                return value
        except:
            return None

    # if datatype is not specified, return the entry
    return value


def process_games(history):
    """
    Go through the history tab, and process each game entry and store it
    :param history: the history tab of the Google Sheet
    :return: a list of dicts of game data
    """
    # Record which tab the record came from
    source = history.title

    games = list()
    rows = history.get_all_values()
    for row in rows[3:]:
        # If the line is blank, skip the whole row
        if not any(row):
            continue

        game = dict()
        game['tab'] = source

        val = get_value(row[0],datatype='date')
        if val:
            game['date'] = hero.util.seconds_since_epoch(hero.util.get_date_value(val))
            game['date_readable'] = val # raw date
            game['dateparts'] = val.split('-') # processed date (YYYY, MM, DD)
        else:
            # if there's no recognizeable date (such as adding extra header rows, or completely fucking up the data format
            # then skip the row, we don't know when the event was
            continue

        val = get_value(row[1])
        if val:
            game['event'] = val
            # record the raw event data, for comparison to the nornalized data
            game['event_raw'] = val

        val = get_value(row[2])
        if val:
            game['location'] = val

        val = get_value(row[3])
        if val:
            game['host_league'] = val

        val = get_value(row[4])
        if val:
            game['high_seed'] = val

        val = get_value(row[5])
        if val:
            game['low_seed'] = val

        val = get_value(row[6], enum=known_associations)
        if val:
            game['assn'] = val

        val = get_value(row[7], enum=game_types)
        if val:
            game['type'] = val

        val = get_value(row[8], enum=known_roles)
        if val:
            game['position'] = val

        val = get_value(row[9], enum=known_roles)
        if val:
            game['second_position'] = val

        val = get_value(row[10], enum=['Y'])
        if val:
            game['positional_software'] = val

        # tack the rest of the information on as "notes"
        game['notes'] = ':'
        val = get_value(row[11])
        if val:
            game['notes'] += val + ':'
        val = get_value(row[12])
        if val:
            game['notes'] += val + ':'
        val = get_value(row[13])
        if val:
            game['notes'] += val + ':'

        games.append(game)
    return games


def record_failure(url, reason, permanent_failure=None):
    """
    This logs failed attempt to reach a URL
    :param url: URL that was attempted
    :param reason: log a message, if there's a failure message
    :param permanent_failure: text label, if it's known to be a permanent failure type
    """
    # if there's an entry already, load it first, otherwise initialize a blank one
    if url in faileddb:
        record = faileddb[url]
        record['num_attempts'] += 1
        if 'permanent_failure' not in record:
            record['permanent_failure'] = ""
    else:
        record = dict()
        record['_id'] = url
        record['num_attempts'] = 1
        record['failure_reason'] = reason
        record['permanent_failure'] = ""

    record['last_attempt'] = time.time()
    record['last_attempt_readable'] = time.ctime()
    if permanent_failure:
        record['permanent_failure'] = permanent_failure
    faileddb[url] = record


def load_from_sheets(gconn, offdb, url):
    """
    This function takes a Google Sheets URL and using the API, parses it and returns a dict structure
    that represents the information taken from the sheet in a format suitable for loading into the couchdb (JSON).
    Currently supporting history docs version: 2.x (only)
    :param gconn: the connection to the Google Sheets API
    :param offdb: the Officials database
    :param url: the URL of the history document to be loaded
    :return: a tuple: (True, document) or (False, error string)
    """
    # open a new Google Sheet
    original_url = url
    # if the url has been tried before but it was flagged as failing to load for an unidentified problem recently, skip them
    if (url in faileddb) and (faileddb[url]['last_attempt'] > too_soon_to_retry_failures):
        return False, "URL was attempted but failed too recently, skipping: {}".format(url)

    # if the raw URL is not in the failure database, then unshorten it
    url = unshorten_url(url)

    if not validators.url(url):
        msg = "No valid URL found at {}".format(url)
        record_failure(original_url, msg, "Bogus URL")
        return False, msg
    try:
        sheet = gconn.open_by_url(url)
    except gspread.SpreadsheetNotFound as e:
        # There is a weirdness (likely in the gspread implementation of the Sheets API) that means if there are too many
        # Sheet IDs, a valid sheet might not be found... but if you open the sheet, then it's in the 500 most recently used
        # and will be found... so if loading fails once, open the doc and try again... it might just work 2nd time around!

        # Use the Raw API call to open the sheet, which hopefully puts it in the user's list of recent sheets and thus will be found now
        # this didn't work either...
        # requests.get(url, params='includeGridData=false')

        # this way didn't work:
        # new_url = requests.get(url, headers=http_header).url

        # sheer brute force option seems to work (just maybe not for me with multiple google IDs...)
        # but if we open it in a browser it works... so we force the issue by making a system call
        # TODO: find a way to close the tab afterwards...
        webbrowser.open_new_tab(url)
        time.sleep(12*SECONDS)

        try:
            sheet = gconn.open_by_url(url)
        except gspread.SpreadsheetNotFound as e:
            msg = "Can't find a spreadsheet there - might be v1 or not a Sheets URL: {}".format(url)
            record_failure(original_url, msg)
            return False, msg
        except Exception as e:
            msg = "Something went wrong trying to open the URL: {}".format(str(e))
            record_failure(original_url, msg)
            return False, msg
        # TODO: COPY the file to a temporary location and try again!
    except Exception as e:
        msg = "Something went wrong trying to open the URL: {}".format(str(e))
        record_failure(original_url, msg)
        return False, msg

    # check the history document template version
    # TODO split this off into a v2.py and a v3.py set of helper functions
    ver = get_version(sheet)
    if ver is None:
        msg = "Unidentified Document Format"
        record_failure(url, msg)
        return False, msg
    elif ver == 1:
        msg = "Unsupported (old) Document Format"
        record_failure(url, msg, "Unsupported Format")
        return False, msg
    elif ver != 2:
        msg = "Unsupported (new) Document Format"
        record_failure(url, msg)
        return False, msg

    # officials unique ID is the history doc ID
    id = sheet.id

    # TODO: Uncomment out this block - it's all commented out to force a refresh of the data since the schema changed
    # # does the official exist in the offdb already?
    # if id in offdb:
    #     # print u"Found existing entry for: " + offdb[id]['name']
    #     # if they exist and the force_refresh flag both exists and is set then force a refresh
    #     if ('force_refresh' in offdb[id]) and (offdb[id]['force_refresh']):
    #         print u"Forcing refresh of {}, per db flag".format(offdb[id]['name'])
    #     # if they exist and they were updated more recently than the stale_time limit, then skip them
    #     elif offdb[id]['last_updated'] > stale_time:
    #         # print "They're delightfully fresh, moving on..."
    #         return False, u"{} is still current in the database".format(offdb[id]['name'])
    #         # return False, None
    #     # in all other cases, the record needs to be updated
    #     else:
    #         # print "They're a bit on the nose, getting a new copy..."
    #         pass

    # basic info
    summary = sheet.worksheet("Summary")
    name_list = get_name(sheet)
    print u"Loading: " + name_list[0]
    off = dict()
    off['_id'] = id
    # if the entry is in the db already, add in the revision number for db referential integrity
    # this will only cause a problem if two people are updating the same record concurrently
    if id in offdb:
        off['_rev'] = offdb[id]['_rev']
    # the URL that was loaded
    off['url'] = url
    # if there is an original URL in the db, keep it. If not, use this attempt's original URL
    if 'url_original' in offdb[id]:
        off['url_original'] = offdb[id]['url_original']
    else:
        off['url_original'] = original_url
    off['name'] = unicode(name_list[0])
    if name_list[2]:
        off['govt_name'] = unicode(name_list[2])
    off['template_version'] = 2
    off['last_updated'] = time.time()
    off['last_updated_readable'] = time.ctime()
    off['force_refresh'] = 0

    off['league'] = unicode(get_value(summary.acell('C5').value))
    # TODO find a way to map league to location
    # off['location'] = ""
    # officiating since: currently free text for whatever they put in the cell
    # TODO look at "from dateutil import parser"
    # It might be able to interpret things, otherwise a series of strpfmt calls
    off['officiating_since'] = get_value(summary.acell('J4').value)
    # TODO calulate how many years of officiating, once I work out how to handle the random formats
    # off['officiating_years'] = ''

    # certification info
    ref_level = normalize_cert_value(summary.acell('C7').value)
    nso_level = normalize_cert_value(summary.acell('C8').value)
    if ref_level or nso_level:
        off['cert'] = dict()
        if ref_level:
            off['cert']['ref_level'] = ref_level
        if nso_level:
            off['cert']['nso_level'] = nso_level
        refcert_val = get_value(summary.acell('G7').value)
        nsocert_val = get_value(summary.acell('G8').value)
        cert_endorsements = normalize_cert_endorsements(refcert_val, nsocert_val)
        if cert_endorsements:
            off['cert']['cert_endorsements'] = cert_endorsements
            # list the raw endorsement entries, for comparison against the normalized value
            if refcert_val:
                off['cert']['cert_endorsements_raw'] = 'ref=' + refcert_val + ' '
            if nsocert_val:
                off['cert']['cert_endorsements_raw'] = 'nso=' + nsocert_val

    # insurance information
    number = get_value(summary.acell('C6').value)
    provider = get_value(summary.acell('H6').value)
    if number or provider:
        off['insurance'] = dict()
        off['insurance']['number'] = get_value(summary.acell('C6').value)
        off['insurance']['provider'] = unicode(get_value(summary.acell('H6').value))

    # game information from the main "Game History"
    games = process_games(sheet.worksheet("Game History"))
    off['games'] = list()
    if games:
        off['games'] += games

    # process the "Other History" tab
    if "Other History" in map(lambda x: x.title, sheet.worksheets()):
        games = process_games(sheet.worksheet("Other History"))
        if games:
            off['games'] += games

    return True, off
####################


####################
# Start the meat of the loading work
####################
def process_list(urls):
    """
    Iterate through the list of URLs provided and load them into the database.
    If they had previously failed to load, and there is an entry in the failure database, remove it if it was successfully loaded
    :param urls: list of URLs
    """
    start_time = time.time()

    # get a connection to the Google sheets API
    gconn = init_google_connection()

    # TODO: figure out how to handle max time more consistently - but for now make it 5 minutes less than the credentials last
    max_run_time = (gconn.auth.token_expiry - datetime.datetime.now()).seconds - 5*MINUTES

    # for each history doc, try to load it in the database
    print u"Attempting to load {} URLs".format(len(urls))
    failures = 0
    successes = 0
    for url in urls:
        # print u"attempting {}".format(url)
        loaded_correctly, off = load_from_sheets(gconn, offdb, url)
        if loaded_correctly:
            print u"Processed {}".format(off['name'])
            offdb.save(off)
            successes += 1
            # if there's an entry in the failure database, clean it up
            # TODO: in the future just mark it as successful, and exclude that from the views - so that once things are running smoothly we can track the intermittent errors
            if url in faileddb:
                print u"Removing {} from the failure database".format(url)
                del faileddb[url]
        # this branch returns as failed to load but it's really a success because the entry was skipped for being "fresh"
        elif off and 'still current' in off:
            successes += 1
            # if there's an entry in the failure database, clean it up
            # TODO: in the future just mark it as successful, and exclude that from the views - so that once things are running smoothly we can track the intermittent errors
            if url in faileddb:
                print u"Removing {} from the failure database".format(url)
                del faileddb[url]
        # this branch is for all other fail responses
        else:
            print u"URL {} was not loaded because: {}".format(url, off)
            failures += 1

        # if this runs for a long time, it's more likely to cause timeout / session errors, so if it runs too long, stop
        if (time.time() - start_time) > max_run_time:
            print u"Processing took too long, stopping..."
            break
    records_processed = successes + failures
    if records_processed > 0:
        fail_rate = failures * 100.0 / records_processed
    else:
        fail_rate = 1
    print u"Total time {:.2f}s. Of {} records, {} were processed (failure rate was {:.1f}%)".format((time.time() - start_time), len(urls), records_processed, fail_rate)


def get_urls_from_sheet(sheet, tabname, column, num_header_rows=1):
    """
    Get a list of URLs from a google sheet, in the named tab and the nominated column
    :param sheet: URL of a google sheet (typically an application form)
    :param tabname: name of the tab to look in
    :param column: the column that has the URLs in it
    :param num_header_rows: the number of header rows to skip
    :return: a list of URLs
    """
    start_time = time.time()
    # Open authenticated connection to the Google Sheets API
    gconn = init_google_connection()

    # open a new Google Sheet
    original_sheet = sheet
    sheet = unshorten_url(original_sheet)
    try:
        sheet = gconn.open_by_url(sheet)
    except gspread.SpreadsheetNotFound:
        return "Can't find a spreadsheet there - might be v1 or not a Sheets URL"
    except Exception as e:
        return "Something went wrong trying to open the URL: {}".format(str(e))

    url_list = list()
    if tabname in map(lambda x: x.title, sheet.worksheets()):
        list_sheet = sheet.worksheet(tabname)
        rows = list_sheet.get_all_values()
        for row in rows[num_header_rows:]:
            url_list.append(row[column])
    else:
        print u"Tab {} not found!".format(tabname)
    print u"Time to load list of URLs {:.2f}s".format((time.time() - start_time))
    return url_list


def get_stale_urls(forced_only=False):
    """
    Query the database for entries that are deemed "stale" and return their URLs for reloading
    :param forced_only: if this is set to true, then only returned force_refresh URLs
    :return: list of URLs
    """
    urls = list()
    if not forced_only:
        # return the entries that are stale (not updated since stale_time)
        docquery = offdb.view('info/last_updated')
        for row in docquery[:stale_time]:
            urls.append(row['value'][2])
    # load the entries marked as wanting to force a refresh
    docquery = offdb.view('info/force_refresh')
    for row in docquery:
        urls.append(row['value'])
    return urls


def get_failed_urls(level=0):
    """
    Query the failure database for entries that have failed in the past and return those URLs for reloading.
    Level of links to return:
        0 = Just the links flagged as likely to be recoverable
        1 = The links that are known to be easily recoverable (transient failures like token ran out part way through processing)
        2 = All links, except for the ones that are flagged as permanent failures
        3 = All links in the database
    :param level: what level of links to return (default 0)
    :return: list of URLs
    """
    viewname = 'info/likely_recoverable'
    if level == 1:
        viewname = 'info/easily_recoverable'
    elif level == 2:
        viewname = 'info/all_but_bad_links'
    elif level == 3:
        viewname = 'info/last_attempt'
    return list(set(map(lambda x: x.get('id'), faileddb.view(viewname))))


# if run from the command line, go through the test list of URLs
if __name__ == '__main__':
    # this variable controls which source will be used to load,
    # 'stale': stale db entries
    # 'failed': get a list of the failed URLs to try again
    # tournament short name: named application form
    # anything else: test url list
    # brute force way of handling args, because the google libraries shit themselves if there are unexpected args still...
    source = 'test'
    if len(sys.argv) > 1:
        if 'stale' in sys.argv:
            source = 'stale'
        elif 'failures' in sys.argv:
            source = 'failures'
        elif 'forced' in sys.argv:
            source = 'forced'
        elif 'playoffs2016' in sys.argv:
            source = 'playoffs2016'
        elif 'cc' in sys.argv:
            source = 'cc'
        elif 'spud' in sys.argv:
            source = 'spud'
        if source != 'test':
            index = sys.argv.index(source)
            del sys.argv[index]

    if source == 'stale':
        # entries in the database that are stale and should be refreshed
        urls = get_stale_urls()
        print u'stale urls: {}'.format(len(urls))
    elif source == 'forced':
        # entries in the database that are stale and should be refreshed
        urls = get_stale_urls(forced_only=True)
        print u'stale urls: {}'.format(len(urls))
    elif source == 'failures':
        # URLs that have been unsuccessfully loaded and should be retried
        urls = get_failed_urls(2)
        print u'failed urls: {}'.format(len(urls))
    elif source == 'cc':
        # clover cup 2017
        sheet_url = 'https://docs.google.com/spreadsheets/d/1qTFa3wzi-3HJ_5JGtr12cZiQ6K9U9pWL9UI8TDo0fUU/edit#gid=1023242182'
        urls = get_urls_from_sheet(sheet_url, 'Form Responses 1', 5)
    elif source == 'spud':
        # spudtown 2016
        sheet_url = 'https://docs.google.com/spreadsheets/d/1PpRykoZoqyYLbiLQ4iUXbUhSTvQrkbCTJXBSukIVXEU/edit#gid=607697869'
        urls = get_urls_from_sheet(sheet_url, 'Form Responses 1', 7)
    elif source == 'playoffs2016':
        # playoffs 2016
        sheet_url = 'https://docs.google.com/spreadsheets/d/1hXwxFjFFFiGsDXpnSj2WW6Fls1pqJ9XOFMGOZDy_9FE/edit#gid=848540806'
        # THR tab
        urls = get_urls_from_sheet(sheet_url, 'THR Applicants', 11)
        # THNSO tab
        urls += get_urls_from_sheet(sheet_url, 'THNSO Applicants', 10)
        # Ref tab
        urls += get_urls_from_sheet(sheet_url, 'SO Applicants', 11)
        # NSO tab
        urls += get_urls_from_sheet(sheet_url, 'NSO Applicants', 10)
    else:
        urls = [
            'https://kweerious.com/bouts',
            'https://docs.google.com/spreadsheets/d/1d5vHV4s8JsnO_G1ekadNlnajwZATPpHaoheNfrr_8Zc/edit#gid=1988016352',
            'https://docs.google.com/spreadsheets/d/1ONkn7kH8ogWp461DzU5HDX7kP-u9vBWcnTPlzdKWL3U/edit#gid=1237957861',
            # 'https://docs.google.com/a/belfastrollerderby.co.uk/spreadsheets/d/1Qvg20aEYs9iV2S-te1Tt_FK9McnTVKWRmqRr0yrw9Qo/edit?usp=docslist_api',
        ]

    # remove the duplicate entries
    url_set = set(urls)
    # remove the known bad entries, if this isn't sourced from the failure database:
    if source != 'failures':
        urls = list(url_set - set(map(lambda x: x.get('id'), faileddb.view('info/known_bad_links'))))

    # do the heavy lifting
    process_list(urls)

    # test fragment to demonstrate a view query for how many stale URLs there are in the DB:
    docquery = offdb.view('info/last_updated')
    two_days_stale = docquery[:(time.time() - 2*DAYS)].rows
    ten_days_stale = docquery[:(time.time() - 10*DAYS)].rows
    print u"total number = {}\n2 days (test) stale {}\n10 days (fully) stale {}".format(docquery.total_rows,
                            len(two_days_stale), len(ten_days_stale))

    # for good measure, automatically replicate to the local couchdb, if it exists
    replication_flag = False
    replication_flag = True
    if replication_flag:
        local_couch = couchdb.Server(local_couch_server)
        replicator = local_couch['_replicator']

        refresh_views = ['replicate_hero_with_great_machine', 'replicate_failures_with_great_machine']
        for view in refresh_views:
            record = replicator[view]
            record['_replication_state'] = ''
            replicator[view] = record

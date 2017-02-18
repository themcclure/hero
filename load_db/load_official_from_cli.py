from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
from tinydb import TinyDB, Query
import gspread
import time
import requests


# Initialize authentication with Google
CLIENT_ID = '48539282111-07fidfl1225gaiqk49ubb6r1fr21npln.apps.googleusercontent.com'
CLIENT_SECRET = 'CQ6-3PPwUjB6nZeYujAuqcWo'

# Set scope of permissions to accessing spreadsheets
scope = ['https://spreadsheets.google.com/feeds', 'https://docs.google.com/feeds']
redirect_uri = 'http://example.com/auth_return'
redirect_uri = 'http://localhost:8080'

# Initialize the "flow" of the Google OAuth2.0 end user credentials
flow = OAuth2WebServerFlow(client_id=CLIENT_ID,
                           client_secret=CLIENT_SECRET,
                           #scope='https://spreadsheets.google.com/feeds https://docs.google.com/feeds',
                           scope=scope,
                           redirect_uri=redirect_uri)

# store the credential, so you don't keep bugging the user
storage = Storage('creds.data')

# Authenticate end user from file first, if the credentials are valid
# NOTE: Flows don't seem to return a refresh token, so when the access
# token expires, you have to invoke user interaction
credentials = storage.get()
if not credentials or credentials.invalid or (credentials._expires_in() == 0):
    credentials = run_flow(flow, storage)

# Open authenticated connection to the Google Sheets API
gc = gspread.authorize(credentials)

########
# test URLs
list_of_urls = [
"https://docs.google.com/spreadsheets/d/1zJv0FYxoiC7YwgqIHmIsN_0h1UGkATi55hNDhM8WiSc/edit#gid=1988016352",
"https://docs.google.com/spreadsheets/d/1kG9QTdus7LbpZP-3L9fNvwQ0nVpUUXyw7m7hpKSBH-E/edit#gid=2008460745",
"http://goo.gl/iR9kn2",
########
]

########
# initialize config items
#
# reload only if the last_load time is older than the stale time 
SECONDS = 1
MINUTES = 60*SECONDS
HOURS = 60*MINUTES
DAYS = 24*HOURS
stale_time = time.time() - 5*MINUTES
stale_time = time.time() - 30*SECONDS
########

# utility function to unshorten URLs
def unshorten_url(url):
    return requests.head(url, allow_redirects=True).url

#initialize datastores
offdb = TinyDB('officials_db.json')
OffQuery = Query()
# TODO: add in aliases db to track alternative names for finding them later
# TODO: add in metadata db to provide lookups

# for each history doc, try to load it in the database
for url in list_of_urls:
    # open a new Google Sheet
    url = unshorten_url(url)
    sheet = gc.open_by_url(url)

    # officials unique ID is the history doc ID
    id = sheet.id

    # does the official exist in the offdb already?
    q = offdb.search(OffQuery.id == id)
    if len(q) > 0:
        print "Found existing entry for: " + q[0]['name']
        #print "last updated at: " + time.ctime(q[0]['last_updated'])
    
        # if they exist and the information is "fresh" then move along
        if q[0]['last_updated'] > stale_time:
            print "They're delightfully fresh, moving on..."
            continue
        else:
            print "They're a bit on the nose, getting a new copy..."
            offdb.remove((OffQuery.id == id))

    summary = sheet.worksheet("Summary")
    name = summary.acell('C4').value
    print "Loading: " + name

    info_sheet = sheet.worksheet("Instructions")
    template_update = info_sheet.acell('A104').value

    # remove the entry first, to be replaced with the updated one
    # maybe one day there will be a historical track of the snapshots
    off = { }
    off['id'] = id
    off['name'] = name
    off['template_version'] = 2
    off['last_updated'] = time.time()

    ref_level = summary.acell('C7').value
    nso_level = summary.acell('C8').value
    if ref_level or nso_level:
        off['cert'] = { }
        if ref_level: off['cert']['ref_level'] = ref_level
        if nso_level: off['cert']['nso_level'] = nso_level
        cert_endorsements = summary.acell('G7').value.split() + summary.acell('G8').value.split()
        if len(cert_endorsements) > 0: 
            off['cert_endorsements'] = cert_endorsements

    off['league'] = summary.acell('C5').value
    off['location'] = ""

    # currently free text for whatever they put in the cell
    # TODO: look at "from dateutil import parser"
    # It might be able to interpret things, otherwise a series of strpfmt calls
    off['officiating_since'] = summary.acell('J4').value
    #off['officiating_years'] = ""

    off['insurance'] = { }
    off['insurance']['number'] = summary.acell('C6').value
    off['insurance']['provider'] = summary.acell('H6').value
    offdb.insert(off)


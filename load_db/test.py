from pydrive.drive import GoogleDrive
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import gspread
import time
import datetime


####################
# max run time before timing out:
SECONDS = 1
MINUTES = 60*SECONDS
HOURS = 60*MINUTES
DAYS = 24*HOURS
max_run_time = 15*MINUTES
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

# if run from the command line, go through the test list of URLs
if __name__ == '__main__':
    gauth = init_google_connection()
    drive = GoogleDrive(gauth)
    x = drive.ListFile().GetList()
    print x
"""
All the methods and config for accessing and reading Google docs 
"""
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.tools import argparser
from oauth2client.file import Storage
import gspread
import os
import re
import requests
import time
import datetime
import hero.config
import hero.util

__author__ = 'hammer'


max_run_time = hero.config.max_run_time
DEBUG = hero.config.DEBUG
get_string_value = hero.util.get_string_value


####################
# web handling definitions
http_protocol = 'http://'
https_protocol = 'https://'
sheets_url_core = 'docs.google.com/spreadsheets/d/'
sheets_url = https_protocol + sheets_url_core
http_header = {'user-agent': 'hero/1.0'}
####################


####################
# Initialize authentication with Google
def init_google_connection(creds_file='creds.data'):
    """
    Authenticate end user from file first, if the credentials are valid
    NOTE: Flows don't seem to return a refresh token, so when the access
    token expires, you have to invoke user interaction
    
    :param creds_file: the location of the credentials file to use
    :return: authorized connection object
    """
    # shared secrets
    CLIENT_ID = '48539282111-07fidfl1225gaiqk49ubb6r1fr21npln.apps.googleusercontent.com'
    CLIENT_SECRET = 'CQ6-3PPwUjB6nZeYujAuqcWo'

    # Set scope of permissions to accessing spreadsheets
    scope = ['https://spreadsheets.google.com/feeds',
             'https://docs.google.com/feeds',
             # 'https://sheets.googleapis.com',
             # 'https://drive.googleapis.com',
             # 'https://www.googleapis.com/auth/spreadsheets',
             # 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
             ]
    redirect_uri = 'http://localhost'

    # Initialize the "flow" of the Google OAuth2.0 end user credentials
    # add the arg --noauth_local_webserver to prompt user for auth code, but I think it needs more Google config to get working
    flags = argparser.parse_args(
        '--auth_host_name localhost --auth_host_port 8080'.split())
    flow = OAuth2WebServerFlow(client_id=CLIENT_ID,
                               client_secret=CLIENT_SECRET,
                               scope=scope,
                               redirect_uri=redirect_uri)

    # store the credential, so you don't keep bugging the user, creating the credentials file if it doesn't exist
    if not os.path.isfile(creds_file):
        file(creds_file, 'w')
    storage = Storage(creds_file)

    credentials = storage.get()
    # refresh credentials if they're not valid
    if not credentials or credentials.invalid or credentials.access_token_expired or DEBUG:
        credentials = run_flow(flow, storage, flags)

    # if the token is valid but will expire before the max_run_time allowed, then refresh them
    token_expires = (credentials.token_expiry - datetime.datetime.now()).seconds
    if DEBUG: print u"Token valid - expires in {} seconds.".format(token_expires)
    if token_expires < max_run_time*2:
        credentials = run_flow(flow, storage, flags)

    # Return an authenticated connection to the Google Sheets API
    return gspread.authorize(credentials)
####################


####################
# Gets the selected columns from the named tab in the given sheet's URL
def get_columns_from_sheet(sheet_url, tabname, num_header_rows, min_columns, columns):
    """
    Get a list of columns from a google sheet, in the named tab and the nominated columns
    
    :param sheet_url: full URL of a google sheet (typically an application form)
    :param tabname: name of the tab to look in
    :param num_header_rows: the number of header rows to skip
    :param min_columns: the minimum number of columns to have data in to provide a valud record
    :param columns: list of the columns to return (in the order supplied)
    :return: a list of column data (list of lists)
    """
    start_time = time.time()

    # Open the sheet
    sheet = open_by_url(sheet_url)
    data_list = list()
    tablist = map(lambda x: x.title, sheet.worksheets())
    if tabname in tablist:
        list_sheet = sheet.worksheet(tabname)
        rows = list_sheet.get_all_values()
        for row in rows[num_header_rows:]:
            row_data = list()
            cols_processed = 0
            missing_min_cols = False
            for col in columns:
                datum = get_string_value(row[col])
                cols_processed += 1
                # TODO: This should change to append the None values to (it's a legit entry)
                # TODO: although maybe better to be "mandatory columns" and "other columns"
                # TODO: How to handle loading the whole tab?
                if datum:
                    row_data.append(datum)
                else:
                    if cols_processed <= min_columns:
                        missing_min_cols = True
                    continue
            # if it's not missing any of the minimum columns, then add it to the list
            if not missing_min_cols:
                data_list.append(row_data)
    else:
        print u"Tab {} not found!".format(tabname)

    if DEBUG: print u"Time to load list of URLs {:.2f}s".format((time.time() - start_time))
    return data_list


####################
def get_sheet_data(sheet, tabname):
    """
    Take an open spreadsheet object and return the contents of the named tab.
    
    :param sheet: a Google Sheets object 
    :param tabname: the name of the tab to return the contents
    :return: a list of column data (list of lists)
    """
    # Open authenticated connection to the Google Sheets API
    gconn = init_google_connection()

    # open a new Google Sheet
    try:
        sheet = gconn.open_by_key(sheet)
    except gspread.SpreadsheetNotFound:
        raise Exception(u"Can't find a spreadsheet with that key - Google can't find it, you don't have permissions or it's not a Google Sheet: {}".format(sheet))
    except Exception as e:
        raise Exception(u"Something went wrong trying to open the sheet: {}".format(str(e)))

    tablist = map(lambda x: x.title, sheet.worksheets())
    if tabname in tablist:
        list_sheet = sheet.worksheet(tabname)
        rows = list_sheet.get_all_values()
        return rows
    else:
        raise Exception(u"Tab not found in the sheet: {}".format(tabname))
####################


####################
def unshorten_url(original_url):
    """
    Takes a URL and does various things to expand it, process it and return a direct URL to the sheet.
    
    :param original_url: the original URL supplied 
    :return: the final URL that is either the original URL, or the final Google sheets URL
    """
    url = original_url

    # Step 0: if there is no protocol information (http(s)://) then add it:
    if http_protocol not in url or https_protocol not in url:
        url = https_protocol + url

    # Step 1: if the URL is a google docs URL, return it (this is kind of the chorus)
    if sheets_url_core in url:
        return url

    # Step 2: strip out officiating history docs that are housed by a google apps domain -
    # Thanks to Belfast Roller Derby and Jean-Quad Grand Slam for this oddity
    pattern = u'(https://docs.google.com/a/)(.+/)(spreadsheets/d/.+$)'
    parts = re.match(pattern, url)
    if parts:
        parts = parts.groups()
        url = u'https://docs.google.com/' + parts[2]

    if sheets_url_core in url:
        return url

    # Step 3: if this is a HTML view (because they're fuckers who hate life), then try to get the spreadsheet view
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
        if sheets_url_core in url:
            return url

    if sheets_url_core in url:
        return url

    # Step 4: quick and safe, high level processing of the URL to see try to follow any redirects in the header response
    try:
        url = requests.head(url, headers=http_header, allow_redirects=True).url
    except requests.ConnectionError as e:
        # print u"error {}".format(str(e))
        # raise e
        pass

    if sheets_url_core in url:
        return url

    # TODO: Insert the rest of the unshortening options
    # TODO: maybe add in the brute force open-in-browser
    # TODO: add in a final brute force make-a-file-copy option

    # if nothing else matches, return the original URL and let it fail
    return original_url
####################


####################
def open_by_url(sheet_url):
    """
    Opens a Google Sheet API connection to the spreadsheet given by URL.
    Raises an exception if a spreadsheet can't be found or opened.
    :param sheet_url: the URL of the sheet
    :return: a list of [gspread.Spreadsheet object, final URL used to open that sheet] 
    """
    # Open authenticated connection to the Google Sheets API
    gconn = init_google_connection()

    unshortened_url = unshorten_url(sheet_url)

    # open a new Google Sheet
    try:
        sheet = gconn.open_by_url(unshortened_url)
    except gspread.SpreadsheetNotFound:
        raise Exception(u"Can't find a spreadsheet with that URL - Google can't find it, you don't have permissions or it's not a Google Sheet: {}".format(sheet_url))
    except Exception as e:
        if str(e):
            raise Exception(u"Something went wrong trying to open the sheet: {}. Error was: {}".format(sheet_url, str(e)))
        else:
            raise Exception(u"Something went wrong trying to open the sheet: {}. Error was not provided".format(sheet_url))
    if sheet:
        return [sheet, unshortened_url]
####################


####################
def open_by_key(sheet_key):
    """
    Opens a Google Sheet API connection to the spreadsheet given by Google Docs key.
    Raises an exception if a spreadsheet can't be found or opened.
    :param sheet_key: the key of the sheet
    :return: a gspread.Spreadsheet object 
    """
    # Open authenticated connection to the Google Sheets API
    gconn = init_google_connection()

    # open a new Google Sheet
    try:
        return gconn.open_by_key(sheet_key)
    except gspread.SpreadsheetNotFound:
        raise Exception(u"Can't find a spreadsheet with that key - Google can't find it, you don't have permissions or it's not a Google Sheet: {}".format(sheet_key))
    except Exception as e:
        if str(e):
            raise Exception(u"Something went wrong trying to open the sheet: {}. Error was: {}".format(sheet_key, str(e)))
        else:
            raise Exception(u"Something went wrong trying to open the sheet: {}. Error was not provided".format(sheet_key))
####################


if __name__ == '__main__':
    l = hero.thegoogles.get_columns_from_sheet('https://docs.google.com/spreadsheets/d/1BxUoYLTNnoa-wnuVjt2qA2JMlwjUrivYCubjWMcmqQ0/edit#gid=1232669535', 'Form Responses 1', [2, 10])

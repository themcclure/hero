"""
Module for working with an official's history document.
Loading, querying, filtering.
"""
import hero.config
import hero.thegoogles
import hero.util
import time
import re


# objects
offdb = hero.config.offdb
faileddb = hero.config.faileddb
endorsements = hero.config.endorsements
known_associations = hero.config.known_associations
game_types = hero.config.game_types
known_roles = hero.config.known_roles2
too_soon_to_retry_failures = hero.config.too_soon_to_retry_failures
# functions
get_template_version = hero.util.get_template_version
get_string_value = hero.util.get_string_value
get_date_value = hero.util.get_date_value
open_by_url = hero.thegoogles.open_by_url
open_by_key = hero.thegoogles.open_by_key


now = int(time.time())
now_readable = time.ctime()


# build the list of URLs (original and final) to map to the entry in the officials db
officials_by_url = dict(map(lambda x: (x['key'], x['id']), offdb.view('info/by_url')))


def load_by_url(original_url, stale_date=None):
    """
    Take a URL and return an dict of an official's history.
    It looks for an up to date database entry, and if one is not found - it attempts to save the document into the database.
    If the optional stale_date is not given, or the official exists with a force_refresh flag set, then always refresh the database.
    
    Throws an exception if history document can't be loaded, and adds an entry to the failuredb.
    
    :param original_url: URL string
    :param stale_date: time tick representation of a date. If the database entry is older than this, refresh it
    :return: a dict containing all the official's history data
    """
    off = dict()
    oringinal_off = dict()
    updated = False

    # if the url has been tried before but it was flagged as failing to load for an unidentified problem recently,
    # OR if the entry is marked with a permanent failure, then skip them
    if original_url in faileddb and (faileddb[original_url]['last_attempt'] > too_soon_to_retry_failures
                                     or faileddb[original_url]['permanent_failure']):
            raise Exception(u"Found URL in failure DB and it's too soon to retry it: {}".format(original_url))

    if original_url in officials_by_url:
        oringinal_off = offdb[officials_by_url[original_url]]
        if not stale_date:
            print u"There is no stale_date, now updating: {}".format(oringinal_off['name'])
            updated = True
        elif 'force_refresh' in oringinal_off and oringinal_off['force_refresh']:
            print u"force_refresh flag is set, now updating: {}".format(oringinal_off['name'])
            updated = True
        elif oringinal_off['last_updated'] < stale_date:
            print u"They're out of date, now updating: {}".format(oringinal_off['name'])
            updated = True
        else:
            print u"They're up to date: {}".format(oringinal_off['name'])
            off = oringinal_off
    else:
        print u"Didn't find {} - loading them from scratch!".format(original_url)
        updated = True

    if updated:
        newlist = list()
        [sheet, final_url] = open_by_url(original_url)
        off = process_sheet(sheet)
        off['url'] = final_url

        if oringinal_off:
            off['_rev'] = oringinal_off['_rev']
            newlist.append(oringinal_off['url'])
            if 'url_list' in oringinal_off:
                newlist += oringinal_off['url_list']
        off['url_original'] = original_url
        newlist.append(original_url)
        newlist.append(final_url)
        off['url_list'] = list(set(newlist))

        # the update was successful: save the entry into the database, and if it exists in the failuredb then remove it
        offdb[off['_id']] = off
        # TODO: in the future just mark it as successful, and exclude that from the views - so that once things are running smoothly we can track the intermittent errors
        if original_url in faileddb:
            print u"Removing {} from the failure database".format(original_url)
            del faileddb[original_url]

    return off


def process_sheet(sheet):
    """
    Takes an open Google Sheet and loads the officiating history data into a dict and and returns it.
    Currently supports the following versions of officiating history document: v2.x
    Throws an exception if the document can't be loaded.
    :param sheet: an open Sheets object 
    :return: a dict of the official's history data
    """
    off = dict()
    # if the key has been tried before but it was flagged as failing to load for an unidentified problem recently, skip them
    key = sheet.id
    if key in faileddb and faileddb[key]['last_attempt'] > too_soon_to_retry_failures:
        raise Exception(u"Key was attempted but failed too recently, skipping: {}".format(key))

    # check the history document template version
    ver = get_template_version(sheet)
    if ver is None:
        msg = u"Unidentified Document Format for {}".format(key)
        record_failure(key, msg, permanent=True, reason="Unsupported Format")
        raise Exception(msg)
    elif ver == 1:
        msg = u"Unsupported (old) Document Format for {}".format(key)
        record_failure(key, msg, permanent=True, reason="Unsupported Format")
        raise Exception(msg)
    elif ver == 2:
        off = process_v2_sheet(sheet)
    else:
        msg = u"Unidentified Document Format for {}".format(key)
        record_failure(key, msg, permanent=True, reason="Unsupported Format")
        raise Exception(msg)
    return off


def record_failure(identifier, msg, permanent=False, reason=None):
    """
    When a record fails to load, log it in the failures database. This is to make sure we don't keep trying to load
    documents that will fail. For (semi-)permanent errors, flag them as permanant and they won't be retried again.
    For other failures they will be skipped for a period of time: hero.config.too_soon_to_retry_failures
    :param identifier: this will be a URL or a document key (depending on how it was loaded)
    :param msg: this is the message associated with the type of failure
    :param permanent: defaults to False - set to True to mark this as a permanent failure
    :param reason: defaults to None - set this for the class of permanent failure
    """
    # if there's an entry already, load it first, otherwise initialize a blank one
    if identifier in faileddb:
        record = faileddb[identifier]
        record['num_attempts'] += 1
    else:
        record = dict()
        record['_id'] = identifier
        record['num_attempts'] = 1
        record['failure_reason'] = msg
        record['permanent_failure'] = ""

    record['last_attempt'] = now
    record['last_attempt_readable'] = now_readable
    if permanent:
        print u'Logged a permanent failure: {}'.format(msg)
        record['permanent_failure'] = reason
    else:
        print u'Logged a transient failure: {}'.format(msg)

    faileddb[identifier] = record


def process_v2_sheet(sheet):
    """
    Takes in a sheet that's from the version 2 template and loads the officiating history data into a dict and and returns it.
    Throws an exception if the document can't be loaded.
    :param sheet: an open Sheets object 
    :return: a dict of the official's history data
    """
    # officials unique ID is the history doc ID
    id = sheet.id

    # basic info
    summary = sheet.worksheet("Summary")
    name_list = hero.util.get_name(get_string_value(summary.acell('C4').value),
                                   get_string_value(summary.acell('C3').value), get_string_value(sheet.title))
    print u"Loading: " + name_list[0]
    off = dict()
    off['_id'] = id
    # if the entry is in the db already, add in the revision number for db referential integrity
    # this will only cause a problem if two people are updating the same record concurrently
    if id in offdb:
        off['_rev'] = offdb[id]['_rev']

    off['name'] = unicode(name_list[0])
    if name_list[2]:
        off['govt_name'] = unicode(name_list[2])
    off['template_version'] = 2
    off['last_updated'] = now
    off['last_updated_readable'] = now_readable
    off['force_refresh'] = 0

    off['league'] = unicode(get_string_value(summary.acell('C5').value))

    # TODO find a way to map league to location
    # off['location'] = ""

    # officiating since: currently free text for whatever they put in the cell
    # TODO look at "from dateutil import parser"
    # It might be able to interpret things, otherwise a series of strpfmt calls
    off['officiating_since'] = get_string_value(summary.acell('J4').value)
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
        refcert_val = get_string_value(summary.acell('G7').value)
        nsocert_val = get_string_value(summary.acell('G8').value)
        cert_endorsements = normalize_cert_endorsements(refcert_val, nsocert_val)
        if cert_endorsements:
            off['cert']['cert_endorsements'] = cert_endorsements
            # list the raw endorsement entries, for comparison against the normalized value
            if refcert_val:
                off['cert']['cert_endorsements_raw'] = 'ref=' + refcert_val + ' '
            if nsocert_val:
                off['cert']['cert_endorsements_raw'] = 'nso=' + nsocert_val

    # insurance information
    number = get_string_value(summary.acell('C6').value)
    provider = get_string_value(summary.acell('H6').value)
    if number or provider:
        off['insurance'] = dict()
        off['insurance']['number'] = get_string_value(summary.acell('C6').value)
        off['insurance']['provider'] = unicode(get_string_value(summary.acell('H6').value))

    # game information from the main "Game History"
    games = process_v2_games(sheet.worksheet("Game History"))
    off['games'] = list()
    if games:
        off['games'] += games

    # process the "Other History" tab
    if "Other History" in map(lambda x: x.title, sheet.worksheets()):
        games = process_v2_games(sheet.worksheet("Other History"))
        if games:
            off['games'] += games

    return off


def process_v2_games(history):
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

        try:
            val = get_date_value(row[0])
        except Exception as e:
            # if there's no recognizeable date (such as adding extra header rows, or completely fucking up the data format
            # then skip the row, we don't know when the event was
            print u"This is not a recognizeable date: {}".format(row[0])
            continue
        else:
            game['date'] = hero.util.seconds_since_epoch(val)
            game['date_readable'] = val.strftime('%Y-%m-%d')  # string date in "YYYY-MM-DD" format
            game['dateparts'] = game['date_readable'].split('-')  # processed date (YYYY, MM, DD)

        val = get_string_value(row[1])
        if val:
            game['event'] = val
            # record the raw event data, for comparison to the nornalized data
            game['event_raw'] = val

        val = get_string_value(row[2])
        if val:
            game['location'] = val

        val = get_string_value(row[3])
        if val:
            game['host_league'] = val

        val = get_string_value(row[4])
        if val:
            game['high_seed'] = val

        val = get_string_value(row[5])
        if val:
            game['low_seed'] = val

        val = get_string_value(row[6], enum=known_associations)
        if val:
            game['assn'] = val

        val = get_string_value(row[7], enum=game_types)
        if val:
            game['type'] = val

        val = get_string_value(row[8], enum=known_roles.keys())
        if val:
            game['position'] = val

        val = get_string_value(row[9], enum=known_roles.keys())
        if val:
            game['second_position'] = val

        val = get_string_value(row[10], enum=['Y'])
        if val:
            game['positional_software'] = val

        # tack the rest of the information on as "notes"
        game['notes'] = ':'
        val = get_string_value(row[11])
        if val:
            game['notes'] += val + ':'
        val = get_string_value(row[12])
        if val:
            game['notes'] += val + ':'
        val = get_string_value(row[13])
        if val:
            game['notes'] += val + ':'

        games.append(game)
    return games


def normalize_cert_endorsements(refcert_string, nsocert_string):
    """
    Takes the string from the cert endorsement cells and returns a list of standard endorsements, plus the strings that
    were not recognized
    :param refcert_string: the raw ref string from the endorsement cells
    :param nsocert_string: the raw NSO string from the endorsement cells
    :return: sorted list of recognized endorsements, plus a split of the remaining entries
    """
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
    cert_string = get_string_value(cert_string)
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


if __name__ == '__main__':
    try:
        # url = 'https://docs.google.com/spreadsheets/d/1zJv0FYxoiC7YwgqIHmIsN_0h1UGkATi55hNDhM8WiSc/edit#gid=1988016352'  # Trauma
        # url = 'https://docs.google.com/spreadsheets/d/1d5vHV4s8JsnO_G1ekadNlnajwZATPpHaoheNfrr_8Zc/edit#gid=1988016352'  # Reed
        url = 'https://docs.google.com/spreadsheets/d/1GbJqpJmp_VWNvEwHpiWG-e7qAE9z0lF45L4xfeC5k0Y/edit?usp=sharing'  # bard
        # url = 'https://docs.google.com/spreadsheets/d/15ctyA98l9IL0c7b2kbI3GgnDf4Irgqp4uEYiwypCpJ0/edit?usp=sharing_eid&ts=564b876a'  # cosmo
        # url = 'http://donthaveone.com'  # Someone trying to avoid having a history
        # url = 'http://tinyurl.com/BenWaGames'
        load_by_url(url, 10000000000)
    except Exception as e:
        print u"Loaing the URL gave me heartburn, and this is its name: {}".format(str(e))

"""Utility functions"""
import hero.config
import datetime
import re
blank_entries = hero.config.blank_entries


####################
# General purpose utility functions
####################


####################
def get_date_value(value):
    """
    Takes a string, and returns the normalized date value, or None if the content is equivaluent to the "empty string".
    :param value: raw spreadsheet value
    :return: interpreted value, as a datetime
    """
    # tidy up the data entry
    value = get_string_value(value)

    if not value:
        raise Exception(u"{} is not a date".format(value))

    # normalize the date separators, and ignore any time inputs after the date:
    try:
        value = '-'.join(re.match('^(\d+)\D(\d+)\D(\d+)', value).groups())
    except Exception as e:
        raise e

    # this will return the datetime object, if the input is a valid ISO date format:
    try:
        newval = datetime.datetime.strptime(value,'%Y-%m-%d')
    except Exception as e:
        pass
    else:
        return newval

    # this will return the datetime object, if the input is (incorrectly) given in the US date format:
    try:
        newval = datetime.datetime.strptime(value, '%m-%d-%Y')
    except Exception as e:
        raise e
    else:
        return newval


####################
def get_string_value(value, enum=None):
    """
    Takes a string, and returns the string value, or None if the content is equivaluent to the "empty string".
    If there is an enum list supplied, it will also only return elements found in that list.
    :param value: raw spreadsheet value
    :param enum: a list containing valid values
    :return: interpreted value, as a string
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

    # if datatype is not specified, return the entry
    return value


####################
def seconds_since_epoch(value):
    """
    Takes a datetime objet and returns an integer number of seconds since epoch
    
    :param value: datetime object 
    :return: integer number of seconds since epoch
    """
    return int((value - datetime.datetime.utcfromtimestamp(0)).total_seconds())


####################
def normalize_positions(positions_list):
    """
    Takes a list of lists of raw positions applied for, and returns a normalized list of positions applied for
    
    :param positions_list: list of lists
    :return: normalized list of positions applied for  
    """
    # parse out all the individual list entries in the list
    positions = list()
    for pos in positions_list:
        positions += map(get_string_value, pos.split(','))

    # match for known positions
    found_positions = list()
    missing_positions = list()
    # if we go for fuzzy matching, this is a good starting place:
    # fuzzywuzzy.process.extractOne('Head NSO and/or CHNSO', hero.config.known_aliases,score_cutoff=90)
    for pos in positions:
        # If the position title is formatted on the application form like "CHR: Crew Head Referee", then take the first bit (from: CC2016)
        pos = pos.split(':')[0]

        # first look to see if this is a role we can ignore
        if pos in hero.config.ignore_roles:
            continue
        # first look for multi-role matches:
        elif pos in hero.config.multi_role_aliases:
            found_positions += hero.config.multi_role_aliases[pos]
        # now look for single role matches:
        elif pos in hero.config.known_roles2:
            found_positions.append(pos)
        # then look for single role aliases:
        elif pos in hero.config.known_aliases:
            found_positions.append(hero.config.known_aliases[pos])
        else:
            missing_positions.append(pos)
            print u"found missing one: {}".format(pos)

    # tidy up duplicates:
    found_positions = list(set(found_positions))
    missing_positions = list(set(missing_positions))

    if missing_positions:
        return found_positions + [{'missing': missing_positions}]
    else:
        return found_positions


def get_template_version(sheet):
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
            raise Exception(u"Modified v2 template version")
        elif 'Instructions' not in sheet_list:
            # this is a new history doc it's been modified to delete the instructions tab (a no no)
            raise Exception(u"Modified v2 template version")
        elif any(map(lambda x: x in sheet.worksheet('Instructions').acell('A104').value, hero.config.known_v2_revisions)):
            return 2
        else:
            raise Exception(u"Modified v2 template version")
    elif 'WFTDA Summary' in sheet_list:
        return 1
    else:
        raise Exception(u"Unknown template version")


def get_name(dname, rname, title):
    """
    Picks through the fields looking for a name in preference order of:
        - derby name
        - real name
        - document title
    :param sheet: the connected Google Sheet
    :return: a list of strings with best guess at name, followed by the raw value of each
    """
    # if the name is blank, fall back to real name
    name = dname
    if dname is None:
        name = rname
    # if the name is blank, fall back to doc title
    if name is None:
        name = title
    return [name, dname, rname, title]

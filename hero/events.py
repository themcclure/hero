"""
Take a tournament application sheet and load all the applicants
"""
import hero.config
import hero.util
import hero.official
import hero.thegoogles
import time
import datetime
from dateutil import relativedelta

__author__ = 'hammer'

# objects
offdb = hero.config.offdb
faileddb = hero.config.faileddb
eventdb = hero.config.eventdb
DEBUG = hero.config.DEBUG
too_soon_to_retry_failures = hero.config.too_soon_to_retry_failures
known_roles = hero.config.known_roles2
known_families = hero.config.known_families
now = int(time.time())
now_readable = time.ctime()
# functions
get_columns_from_sheet = hero.thegoogles.get_columns_from_sheet
get_date_value = hero.util.get_date_value
normalize_positions = hero.util.normalize_positions
seconds_since_epoch = hero.util.seconds_since_epoch
record_failure = hero.official.record_failure


def add_event(short_name, name, year, assn, event_type, application_close_date, application_tuples):
    """
    This function creates a new event in the database.
    Raises an Exception if it encounters an error.
    
    :param short_name: short name to refer to the event (eg 'cc2016')
    :param name: full name of the event (eg 'Clover Cup')
    :param year: year the event ran (eg '2016')
    :param assn: Association of record for the event (eg 'WFTDA')
    :param event_type: Type of event (eg 'Sanc')
    :param application_close_date: Date that the applications closed
    :param application_tuples: list of tuples, one for each application form (TH, CH or multiple app forms). Each tuple will be in this format:
            (
            form_url, 
            tab_name, 
            applicant_name_column,
            applicant_url_column
            )
    :return: None
    """
    event = dict()
    event['created_readable'] = now_readable
    event['created'] = now
    if short_name in eventdb:
        if not eventdb[short_name]['force_refresh']:
            raise Exception('Event name {} already exists'.format(short_name))
        else:
            event['_rev'] = eventdb[short_name]['_rev']
            event['created_readable'] = eventdb[short_name]['created_readable']
            event['created'] = eventdb[short_name]['created']

    event['_id'] = short_name
    event['name'] = name
    event['year'] = year
    event['assn'] = assn
    event['type'] = event_type
    close_date = get_date_value(application_close_date)
    event['close_date'] = seconds_since_epoch(close_date)
    event['close_date_readable'] = close_date.strftime('%Y-%m-%d')
    event['freeze_date_is_close_date'] = 1  # 1 for freeze_date is close_date, 0 for application_date
    event['force_refresh'] = 0
    event['apps_closed'] = 0
    event['forms'] = list()
    for form in application_tuples:
        form_dict = dict()
        form_dict['url'] = form[0]
        form_dict['tab'] = form[1]
        form_dict['name_col'] = form[2]
        form_dict['history_col'] = form[3]
        form_dict['application_date_col'] = form[4]
        form_dict['positions'] = form[5:]
        event['forms'].append(form_dict)

    if DEBUG: print event
    eventdb[short_name] = event


def scan_event_application(event):
    """
    Scan through the application form of the named event, and save the applicant data in the database.

    :param event: name of the event 
    :return: None
    """
    if event not in eventdb:
        print u"Event {} not found!".format(event)
        return

    print u"Scanning event {}".format(event)
    event_dict = eventdb[event]

    if event_dict['apps_closed'] and not event_dict['force_refresh']:
        print u"Event {} is already up to date".format(event)
        return

    officials = list()
    close_date = get_date_value(eventdb[event]['close_date_readable'])
    for form in eventdb[event]['forms']:
        event_url = form['url']
        tab_name = form['tab']
        cols = [form['name_col'], form['history_col'], form['application_date_col']] + form['positions']
        min_cols = 3  # the number of named columns we need as mandatory
        if 'header_rows' in form:
            header_rows = form['header_rows']
        else:
            header_rows = 1

        try:
            off_results = get_columns_from_sheet(event_url, tab_name, header_rows, min_cols, cols)
        except Exception as e:
            print u"Event {} failed to process applicants because: {}".format(event, str(e))
        else:
            for off in off_results:
                official = dict()
                official['name'] = off[0]
                official['url'] = off[1]
                # if the url is already in the list, just use the most recent application
                # TODO: by deleting or overwriting the older entry - but how to find the index?
                if off[1] in [o['url'] for o in officials]:
                    pass
                    # TODO - this doesn't belong here
                    # TODO: if the same person is in two different forms, then append the applied positions to the first one and delete the second one
                app_date = get_date_value(off[2])
                official['application_date'] = seconds_since_epoch(app_date)
                official['application_date_readable'] = app_date.strftime('%Y-%m-%d')
                official['positions_raw'] = ', '.join(off[3:])
                official['positions'] = normalize_positions(off[3:])
                if event_dict['freeze_date_is_close_date']:
                    official['freeze_date'] = seconds_since_epoch(close_date)
                    official['freeze_date_readable'] = close_date.strftime('%Y-%m-%d')
                else:
                    official['freeze_date'] = seconds_since_epoch(app_date)
                    official['freeze_date_readable'] = app_date.strftime('%Y-%m-%d')
                officials.append(official)

    # persist the games to the db
    if officials:
        event_dict['applicants'] = officials
        event_dict['updated'] = now
        event_dict['updated_readable'] = now_readable
        event_dict['force_refresh'] = 1  # TODO: change this to a 0 when not coding/debugging
        if now > seconds_since_epoch(close_date):
            event_dict['apps_closed'] = 1
        else:
            event_dict['apps_closed'] = 0
        eventdb[event] = event_dict
        print u"Found {} applicants".format(len(officials))
    else:
        print u"Did not find any applicants"


def qualify_applicants(event, qualifying_model=None):
    """
    Parse an event, and go through all the applicant for that even.
    For each applicant, go through each position they applied for.
    For each FAMILY check the number of qualifying games they in the qualifying period, and see if they meet minimum 
    experience to qualify.
    For each qualified FAMILY, add the applicant to a list of qualified applicants in the event for each position in that
    family that they applied for.
    
    :param event: Name of the event
    :param qualifying_model: Name of the model to use for qualification
    :return: None
    """
    # TODO: change from a fixed qualification model, to one defined per-event
    qualifying_games = [{'assn': 'WFTDA', 'types': ['Sanc', 'Reg', 'Playoff', 'Champs']}, {'assn': 'MRDA', 'types': ['Sanc', 'Playoff', 'Champs']}]
    qualifying_min_total = 0
    qualifying_min_positional = 1
    qualifying_months = 12
    successful_load = 0
    event_qualified_positions = dict()

    for applicant in eventdb[event]['applicants']:
        print u"Looking for: {}".format(applicant['name'])
        freeze_date = applicant['freeze_date']
        url = applicant['url']
        name = applicant['name']

        # if the url has been tried before but it was flagged as failing to load for an unidentified problem recently, skip them
        if url in faileddb and faileddb[url]['last_attempt'] > too_soon_to_retry_failures:
            print u"Found URL in failure DB and it's too soon to retry it: {}".format(url)
            continue

        try:
            off = hero.official.load_by_url(url, stale_date=freeze_date)
        except Exception as e:
            print u"Can't load {} because: {}".format(name, str(e))
            record_failure(url, str(e))
            continue

        print u'Found {}, they belong to: {}'.format(name, off['league'])
        games = off['games']
        qualifying_period = relativedelta.relativedelta(months=qualifying_months)
        qualifying_date_obj = datetime.datetime.strptime(applicant['freeze_date_readable'], '%Y-%m-%d') - qualifying_period
        qualifying_date = seconds_since_epoch(qualifying_date_obj)
        filtered_games = list()
        for q in qualifying_games:
            filtered_games += [g for g in games if 'assn' in g and g['assn'] in q['assn']
                               and 'type' in g and g['type'] in q['types']
                               and 'position' in g and g['position'] in known_roles
                               and 'date' in g and qualifying_date < g['date'] <= freeze_date]

        if len(filtered_games) < qualifying_min_total:
            # print u"Did not qualify: {} games short".format(qualifying_min_total - len(filtered_games))
            continue

        # print u"Qualified total game count with {} qualified games".format(len(filtered_games))

        for fam in known_families:
            fam_roles = [x for x in filtered_games if x['position'] in known_families[fam]]

            if len(fam_roles) < qualifying_min_positional:
                # print u"Did not qualify for: {}".format(fam)
                continue
            qualified_positions_applied_for = list(set(known_families[fam]) & set(applicant['positions']))
            if not qualified_positions_applied_for:
                # print u"Qualified for {} family - but did not apply for any positions in that family".format(fam)
                continue
            # print u"Qualified for {} within the {} family - with {} games".format(qualified_positions_applied_for, fam, len(fam_roles))
            for pos in qualified_positions_applied_for:
                if pos not in event_qualified_positions:
                    event_qualified_positions[pos] = list()
                event_qualified_positions[pos].append({'name': name,'num_qualified_games_in_family': len(fam_roles),
                                                       'weighted_experience': len([x for x in filtered_games if x['position'] == pos])})

        successful_load += 1

    print u"Found {} applicants, and successfully processed {}".format(len(eventdb[event]['applicants']),successful_load)
    event_entry = eventdb[event]
    # sort the list from most to least experience, base on the weighted_experience
    event_qualified_positions = {y: sorted(event_qualified_positions[y],key=lambda x: x['weighted_experience'],reverse=True) for y in event_qualified_positions.keys()}
    event_entry['qualified_applicants'] = event_qualified_positions
    event_entry['updated'] = now
    event_entry['updated_readable'] = now_readable
    eventdb[event] = event_entry


if __name__ == '__main__':
    try:
        # hero.events.add_event('playoffs2016-th', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2016-00-00', [
        #     (None, None, None, None, None),
        # ])

        # already loaded:
        # hero.events.add_event('playoffs2015-d2-cleveland', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 21),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 22),
        # ])
        # hero.events.add_event('playoffs2015-d2-detroit', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 22),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 23),
        # ])
        # hero.events.add_event('playoffs2015-d1-tucson', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 23),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 24),
        # ])
        # hero.events.add_event('playoffs2015-d1-dallas', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 24),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 25),
        # ])
        # hero.events.add_event('playoffs2015-d1-jax', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 25),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 26),
        # ])
        # hero.events.add_event('playoffs2015-d1-omaha', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 26),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 27),
        # ])
        # hero.events.add_event('playoffs2015-champs', 'WFTDA Playoffs', 2016, 'WFTDA', 'Playoff', '2015-05-31', [
        #     ('https://docs.google.com/spreadsheets/d/1Yjy0fyO5JRGfb-jDiZZtZrSu1Zx0BRD-6MtI8aMgsOc/edit#gid=1900228492', 'Referee Applications ', 2, 14, 0, 27),
        #     ('https://docs.google.com/spreadsheets/d/1Qon6HxF5h7xL5VtlokQlcAVu4rIkSMtw2y9ocKD1FaE/edit#gid=818570689', 'NSO Applications ', 2, 15, 1, 28),
        # ])
        # hero.events.add_event('st2016', 'Spudtown Knockdown', 2016, 'WFTDA', 'Sanc', '2016-02-26', [('https://docs.google.com/spreadsheets/d/1PpRykoZoqyYLbiLQ4iUXbUhSTvQrkbCTJXBSukIVXEU/edit#gid=607697869', 'Form Responses 1', 2, 7, 0, 13, 16)])
        # hero.events.add_event('st2015', 'Spudtown Knockdown', 2016, 'WFTDA', 'Sanc', '2015-04-15', [('https://docs.google.com/spreadsheets/d/1R89TpLvIispPQaFxrBhIQjFpqgnLrTQg5ptwCaM4PTc/edit#gid=425033240', 'Form Responses 1', 2, 7, 0, 12, 13)])
        # hero.events.add_event('rollercon2016', 'RollerCon', 2016, 'WFTDA', 'Sanc', '2016-07-22', [('https://docs.google.com/spreadsheets/d/1DTuBMMwHzv8G1oaLOKivWbifbx0C6Bv4lZhWtVA6Cko/edit#gid=52621778', 'Form Responses 1', 2, 7, 0, 6)])
        # hero.events.add_event('cc2016', 'Clover Cup', 2016, 'WFTDA', 'Sanc', '2016-01-15', [('https://docs.google.com/spreadsheets/d/1BxUoYLTNnoa-wnuVjt2qA2JMlwjUrivYCubjWMcmqQ0/edit#gid=1232669535', 'Form Responses 1', 2, 10, 0, 11)])
        # hero.events.add_event('cc2017', 'Clover Cup', 2017, 'WFTDA', 'Sanc', '2017-01-15', [('https://docs.google.com/spreadsheets/d/1qTFa3wzi-3HJ_5JGtr12cZiQ6K9U9pWL9UI8TDo0fUU/edit#gid=1023242182', 'Form Responses 1', 3, 5, 0, 10)])
        # hero.events.add_event('bigo2017', 'The Big O', 2017, 'WFTDA', 'Sanc', '2017-01-16', [('https://docs.google.com/spreadsheets/d/1RbSiIbP4Isml9cdtq14mPFQT4zfeVBMD3xBnp0B2Q_c/edit#gid=0', 'All responses', 2, 9, 0, 5)])

        # scan_event_application('playoffs2015-d2-cleveland')
        # scan_event_application('playoffs2015-d2-detroit')
        # scan_event_application('playoffs2015-d1-tucson')
        # scan_event_application('playoffs2015-d1-jax')
        # scan_event_application('playoffs2015-d1-dallas')
        # scan_event_application('playoffs2015-d1-omaha')
        # scan_event_application('playoffs2015-champs')
        # scan_event_application('cc2016')
        # scan_event_application('cc2017')
        # scan_event_application('bigo2017')  # needs @themcclure
        # scan_event_application('rollercon2016')  # needs @themcclure
        # scan_event_application('st2015')  # broken?
        # scan_event_application('st2016')  # needs @themcclure
        # qualify_applicants('cc2017')
        # qualify_applicants('cc2016')
        qualify_applicants('st2016')
        # for t in eventdb:
        #     scan_event_application(t)
    except Exception as e:
        print str(e)

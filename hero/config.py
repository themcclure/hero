"""
CONFIG:
List of known Associations, Game Types and Roles
"""
import time
import couchdb

__author__ = 'hammer'

####################
# Dev and logging config
# DEBUG = True
DEBUG = False
####################


####################
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
    'LT',
    'ALTN',
]
known_roles = ref_roles + nso_roles


####################
# BOOTSTRAP CONFIG ONLY
known_roles2 = dict()
known_roles2['THR'] = {'order': 'Referee', 'family': 'TH', 'name': 'Tournament Head Referee', 'active': 1}
known_roles2['CHR'] = {'order': 'Referee', 'family': 'HR', 'name': 'Crew Head Referee', 'active': 1}
known_roles2['HR'] = {'order': 'Referee', 'family': 'HR', 'name': 'Head Referee', 'active': 1}
known_roles2['IPR'] = {'order': 'Referee', 'family': 'IPR', 'name': 'Inside Pack Referee', 'active': 1}
known_roles2['JR'] = {'order': 'Referee', 'family': 'JR', 'name': 'Jammer Referee', 'active': 1}
known_roles2['OPR'] = {'order': 'Referee', 'family': 'OPR', 'name': 'Outside Pack Referee', 'active': 1}
known_roles2['ALTR'] = {'order': 'Referee', 'family': 'ALT', 'name': 'Alt Referee', 'active': 1}

known_roles2['THNSO'] = {'order': 'NSO', 'family': 'TH', 'name': 'Tournament Head NSO', 'active': 1}
known_roles2['CHNSO'] = {'order': 'NSO', 'family': 'HNSO', 'name': 'Crew Head NSO', 'active': 1}
known_roles2['HNSO'] = {'order': 'NSO', 'family': 'HNSO', 'name': 'Head NSO', 'active': 1}

known_roles2['PT'] = {'order': 'NSO', 'family': 'PT', 'name': 'Penalty Tracker', 'active': 1}
known_roles2['PW'] = {'order': 'NSO', 'family': 'PT', 'name': 'Penalty Wrangler', 'active': 1}
known_roles2['PLT'] = {'order': 'NSO', 'family': 'PT', 'name': 'Penalty/Lineup Tracker', 'active': 1}
known_roles2['IWB'] = {'order': 'NSO', 'family': 'PT', 'name': 'Inside Whiteboard', 'active': 1}
known_roles2['OWB'] = {'order': 'NSO', 'family': 'PT', 'name': 'Outside Whiteboard', 'active': 0}

known_roles2['JT'] = {'order': 'NSO', 'family': 'ST', 'name': 'Jam Timer', 'active': 1}
known_roles2['SO'] = {'order': 'NSO', 'family': 'ST', 'name': 'Scoreboard Operator', 'active': 1}
known_roles2['SK'] = {'order': 'NSO', 'family': 'ST', 'name': 'Scorekeeper', 'active': 1}

known_roles2['PBM'] = {'order': 'NSO', 'family': 'PM', 'name': 'Penalty Box Manager', 'active': 1}
known_roles2['PBT'] = {'order': 'NSO', 'family': 'PM', 'name': 'Penalty Box Timer', 'active': 1}
known_roles2['LT'] = {'order': 'NSO', 'family': 'PM', 'name': 'Lineup Tracker', 'active': 1}

known_roles2['ALTN'] = {'order': 'NSO', 'family': 'ALT', 'name': 'Alt NSO', 'active': 1}
# Add then add the full name and short name of the position in as the first aliases
for role in known_roles2:
    known_roles2[role]['alias'] = [role, known_roles2[role]['name']]

# add in all the aliases:
known_roles2['PLT']['alias'].append('PTLT')
known_roles2['PLT']['alias'].append('PT/LT')
known_roles2['PLT']['alias'].append('P/LT')
known_roles2['IWB']['alias'].append('Inside White Board')
known_roles2['SO']['alias'].append('Score Board Operator')
known_roles2['JR']['alias'].append('Jam Referee')
known_roles2['JR']['alias'].append('Jammer Ref')
known_roles2['IPR']['alias'].append('Inside Pack Ref')
known_roles2['OPR']['alias'].append('Outside Pack Ref')
known_roles2['HR']['alias'].append('Head Ref')
known_roles2['CHR']['alias'].append('Crew Head Ref')
known_roles2['LT']['alias'].append('Line Tracker')
known_roles2['LT']['alias'].append('Line Up Tracker')
known_roles2['LT']['alias'].append('Line-Up Tracker')
known_roles2['LT']['alias'].append('Line-up Tracker')

# invert the roles to aliases for lookup, by using a dict comprehension
known_aliases = {alias: role for role in known_roles2.keys() for alias in known_roles2[role]['alias']}

# list all the known families, by using a list comprehension
known_families_list = sorted(list(set([known_roles2[x]['family'] for x in known_roles2])), reverse=True)
# we don't ever care about qualifying to ALT, so remove that from known_families
known_families_list.remove('ALT')
# reverse lookup dict of roles, by the family they're part of:
known_families = {y: [x for x in known_roles2 if known_roles2[x]['family'] == y] for y in known_families_list}

# multi-role aliases
multi_role_aliases = dict()
multi_role_aliases['All'] = known_roles2.keys()
multi_role_aliases['Any Position'] = known_roles2.keys()
multi_role_aliases['I would like to be considered for a role at Championships'] = known_roles2.keys()
multi_role_aliases['Head NSO and/or CHNSO'] = ['CHNSO', 'HNSO']
multi_role_aliases['All Non-Skating Official'] = [pos for pos in known_roles2 if known_roles2[pos]['order'] == 'NSO']
multi_role_aliases['All Skating Official'] = [pos for pos in known_roles2 if known_roles2[pos]['order'] == 'Referee']

# positions to ignore, because they don't map to real roles applied for
ignore_roles = list()
ignore_roles.append('I am applying for a Referee position')
ignore_roles.append('but I am willing to be considered for the selected NSO positions if not accepted as a Referee')
ignore_roles.append('I am applying for a referee position')
ignore_roles.append('but I am willing to consider the selected NSO positions')
ignore_roles.append('Games Tournament Oversight Officer')
ignore_roles.append('Games Official (GTO)')

# TODO: Add it to the database
# to get all the roles by family:
# { x: known_roles2[x] for x in known_roles2.keys() if known_roles2[x]['family'] == 'HNSO'}
# or just the keys:
# { x for x in known_roles2.keys() if known_roles2[x]['family'] == 'HNSO'}
####################

####################
# Certification Endorsements
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


####################
# configure stale_time to determine if the history doc needs to be reloaded
SECONDS = 1
MINUTES = 60*SECONDS
HOURS = 60*MINUTES
DAYS = 24*HOURS
too_soon_to_retry_failures = time.time() - 2*DAYS
# too_soon_to_retry_failures = time.time() # short (debug) time
stale_time = time.time() - 30*DAYS
# stale_time = time.time() - 2*DAYS # short (debug) time

# max run time before timing out:
max_run_time = 15*MINUTES
####################


####################
# Initialize datastores
couch_server = 'http://hero:oreh@10.0.1.61:5984'  # in home URL for docker image on the great machine
# couch_server = 'http://hero:oreh@themcclure.synology.me:59841' # docker image on the great machine
# couch_server = 'http://hero:oreh@heroic.databutler.ca:59841' # docker image on the great machine with a hopefully more reliable DYNDNS
# couch_server = 'https://hero:oreh@couchdb-f40e3a.smileupps.com/' # hosted
local_couch_server = 'http://admin:nimda@127.0.0.1:5984'  # replicated local image'
# couch = couchdb.Server(couch_server)
couch = couchdb.Server(local_couch_server)
offdb = couch['hero']
faileddb = couch['heroic_failures']
eventdb = couch['heroic_events']
# TODO: add in officials-aliases db to track alternative names for finding them later
# TODO: add in metadata db to provide lookups
####################

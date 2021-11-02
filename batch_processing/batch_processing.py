from datetime import datetime

import requests
import json
import time

import configparser

# Initial attempt at some batch processing on a dataverse archive
# not using existing libs that wrap the API
# but use the requests lib a bit like how you would use curl on the commandline.

# Config

# Note: You should have something similar to a VM (with Vagrant or Docker) running Dataverse
# for doing (integration) tests, which you MUST do before running anything on production!

CONFIG_FILE = "../work/config.ini"  # defaults to inside work dir, but maybe have it overidden by environment or commandline params?
# read the ini file from the work dir
# remember to create this 'work' dir and not add it to Git!
config = configparser.ConfigParser()
config.read(CONFIG_FILE)
# for now, use global vars but should be placed in Settings class tha we can pass around
DATAVERSE_API_TOKEN = config.get('DATAVERSE', 'API_TOKEN')
SERVER_URL = config.get('DATAVERSE', 'SERVER_URL')
PIDS_INPUT_FILE = config.get('FILES', 'PIDS_INPUT_FILE')
OUTPUT_DIR = config.get('FILES', 'OUTPUT_DIR')

# thin 'client' functions for http API on the dataverse service, not using a API lib, just the requests lib
# could be placed in a class that also keeps hold of the url and token that we initialise once!
#
# Also note that here we use the PID (persistentId) instead of the internal ID form of the requests.


def get_dataset_metadata(pid):
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    dv_resp = requests.get(SERVER_URL + '/api/datasets/:persistentId/versions/:latest?persistentId=' + pid,
                           headers=headers)
    # Maybe give some more feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    dv_resp.raise_for_status()
    resp_data = dv_resp.json()['data']
    return resp_data


# note that the dataset will become a draft if it was not already
def replace_dataset_metadatafield(pid, field):
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    dv_resp = requests.put(
        SERVER_URL + '/api/datasets/:persistentId/editMetadata?persistentId=' + pid + '&replace=true',
        data=json.dumps(field, ensure_ascii=False),
        headers=headers)
    dv_resp.raise_for_status()


def get_dataset_roleassigments(pid):
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    params = {'persistentId': pid}
    try:
        dv_resp = requests.get(SERVER_URL + '/api/datasets/:persistentId/assignments',
                               params=params,
                               headers=headers)
        dv_resp.raise_for_status()
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data


def delete_dataset_roleassigment(pid, assignment_id):
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    dv_resp = requests.delete(SERVER_URL + '/api/datasets/:persistentId/assignments/' + str(assignment_id)
                              + '?persistentId=' + pid,
                              headers=headers)
    dv_resp.raise_for_status()


def get_dataset_locks(pid):
    dv_resp = requests.get(SERVER_URL + '/api/datasets/:persistentId/locks?persistentId=' + pid)
    # give some feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    dv_resp.raise_for_status()
    resp_data = dv_resp.json()['data']
    return resp_data


def delete_dataset_locks(pid):
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    dv_resp = requests.delete(SERVER_URL + '/api/datasets/:persistentId/locks?persistentId=' + pid,
                              headers=headers)
    dv_resp.raise_for_status()


def publish_dataset(pid):
    version_upgrade_type = "major"  # must be 'major' or 'minor', indicating which part of next version to increase
    headers = {'X-Dataverse-key': DATAVERSE_API_TOKEN}
    dv_resp = requests.post(SERVER_URL + '/api/datasets/:persistentId/actions/:publish?persistentId='
                            + pid + '&type=' + version_upgrade_type,
                            headers=headers)
    dv_resp.raise_for_status()


# this is via the admin api and does not use the token,
# but instead will need to be run on localhost or via an SSH tunnel for instance!
def reindex_dataset(pid):
    dv_resp = requests.get(SERVER_URL + '/api/admin/index/dataset?persistentId=' + pid)
    dv_resp.raise_for_status()
    resp_data = dv_resp.json()['data']
    return resp_data

# processing actions on datasets in dataverse

def reindex_dataset_action(pid):
    print("Try reindexing the dataset")
    reindex_dataset(pid)
    # could log the result
    print("Done")
    return True


def publish_dataset_action(pid):
    # could check if there is a draft first
    print("Try publishing the dataset")
    publish_dataset(pid)
    print("Done")
    return True


# general purpose find and replace for field values in metadata blocks
def replace_metadata_field_value_action(pid, mdb_name, field_name, field_from_value, field_to_value):
    # not always needed when doing a replace,
    # but when you need to determine if replace is needed by inspecting the current content
    # you need to 'get' it first.
    # Another approach would be to do that selection up front
    # and have that generate a list with pids to process 'blindly'.
    resp_data = get_dataset_metadata(pid)
    # print(resp_data['datasetPersistentId'])
    mdb_fields = resp_data['metadataBlocks'][mdb_name]['fields']
    # print(json.dumps(mdb_fields, indent=2))

    # metadata field replacement (an idempotent action I think)
    # replace replace_from with replace_to for field with typeName replace_field
    replace_field = field_name
    replace_from = field_from_value
    replace_to = field_to_value

    replaced = False
    for field in mdb_fields:
        # expecting (assuming) one and only one instance,
        # but the code will try to change all it can find
        if field['typeName'] == replace_field:
            print("Found " + replace_field + ": " + field['value'])
            if field['value'] == replace_from:
                updated_field = field.copy()
                # be save and mutate a copy
                updated_field['value'] = replace_to
                print("Try updating it with: " + updated_field['value'])
                replace_dataset_metadatafield(pid, updated_field)
                print("Done")
                replaced = True
            else:
                print("Leave as-is")
    return replaced


def unlock_dataset_action(pid):
    deleted_locks = False
    resp_data = get_dataset_locks(pid)
    # print(json.dumps(resp_data, indent=2))
    if len(resp_data) == 0:
        print("No locks")
        print("Leave as-is")
    else:
        print("Found locks")
        print(json.dumps(resp_data, indent=2))
        # delete
        print("Try deleting the locks")
        delete_dataset_locks(pid)
        print("Done")
        deleted_locks = True

    return deleted_locks


def delete_roleassigment_action(pid, role_assignee, role_alias):
    deleted_role = False
    resp_data = get_dataset_roleassigments(pid)
    # print(json.dumps(resp_data, indent=2))
    for role_assignment in resp_data:
        assignee = role_assignment['assignee']
        # role_id = role_assignment['roleId']
        alias = role_assignment['_roleAlias']
        print("Role assignee: " + assignee + ', role alias: ' + alias)
        if assignee == role_assignee and alias == role_alias:
            # delete this one
            assignment_id = role_assignment['id']
            print("Try deleting the role assignment")
            delete_dataset_roleassigment(pid, assignment_id)
            print("Done")
            deleted_role = True
        else:
            print("Leave as-is")
    return deleted_role


# pids producing function (extracting from a text file with a pid on each line)

def get_pids_to_process():
    # should read from a file
    # return ["doi:10.80227/test-FT7YRT", "doi:10.80227/test-LP1AWV"]
    pids = []
    with open(PIDS_INPUT_FILE) as f:
        pids = f.read().splitlines()
        # Note that f.readlines() would miss the last 'line' if it has no newline character
    # trim whitespace and remove empty lines...
    return list(filter(lambda item: item.strip(), pids))


# NOTE: maybe use logging for this next file writing?
timestamp_str = '_' + datetime.now().strftime("%Y%m%d_%H%M%S")
mutated_dataset_pids_file = open(OUTPUT_DIR + '/pids_mutated'
                                 + timestamp_str + '.txt', 'w')

# Simple for-loop processing on each dataset with the PID in the list, which is also loaded completely into memory.
# No multi-threading, no streaming no chaining, just plain and simple one thing at a time in a loop.
def batch_process(pids, process_action_func, delay=0.1):
    num_pids = len(pids)  # we read all pids in memory and know how much we have
    print("Start batch processing on {} datasets".format(num_pids))
    print("For each dataset it will use action: {}".format(process_action_func.__name__))
    num = 0
    for pid in pids:
        num += 1
        # show progress, by count numbers etc.
        print("[{} of {}] Processing dataset with pid: {}".format(num, num_pids, pid))
        try:
            mutated = process_action_func(pid)  # delete_contributor_role(pid)
            # log the pids for datatsets that are changed, which might need publishing...
            if mutated:
                mutated_dataset_pids_file.write(pid + '\n')
                mutated_dataset_pids_file.flush()
        except Exception as e:
            print("Stop processing because of an exception:  {}".format(e))
            break  # bail out, but maybe we can make an setting for doing as-much-as-possible and continue
        # be nice and give the server time to recover from our request and serve others...
        if delay > 0 and num < num_pids:
            print("Sleeping for {} seconds...".format(delay))
            time.sleep(delay)
    print("Done processing {} datasets".format(num_pids))


# task functions; do batch processing with actions

# publish is doing a lot after the async. request is returning... sometimes datasets get locked
def publish_dataset_task(pids):
    batch_process(pids, publish_dataset_action, delay=5.0)


def unlock_dataset_task(pids):
    batch_process(pids, unlock_dataset_action, delay=1.5)


# could be fast, but depends on number of files inside the dataset
def reindex_dataset_task(pids):
    batch_process(pids, reindex_dataset_action, delay=1.5)


def delete_contributor_role_for_dataverseadmin_task(pids):
    batch_process(pids, lambda pid: delete_roleassigment_action(pid, "@dataverseAdmin", "contributor"), delay=1.5)


# Example replacement for dccd metadata
def replace_dccd_PI_onbekend_to_XYZ_task(pids):
    batch_process(pids, lambda pid: replace_metadata_field_value_action(pid, "dccd", "dccd-principalInvestigator", "onbekend", "XYZ"), delay=1.5)


# assign the task to be run, looks a bit like it could be a configuration or maybe a commandline parameter
active_task = delete_contributor_role_for_dataverseadmin_task
# unlock_dataset_task  #  publish_dataset_task # delete_contributor_role_for_dataverseadmin_task

# main stuff is running the task
start_time = datetime.now()
pids = get_pids_to_process()
print("Start tasks: {}".format(active_task.__name__))
# run the task
active_task(pids)
print("Done with task")
end_time = datetime.now()
msg = 'Duration: {}'.format(end_time - start_time);
print(msg)
mutated_dataset_pids_file.close()

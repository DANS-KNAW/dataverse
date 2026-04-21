# https://guides.dataverse.org/en/latest/api/native-api.html#add-a-file-to-a-dataset
#
# purpose: a directory label conflicting with an existing file in the root

from datetime import datetime
import json
import requests  # http://docs.python-requests.org/en/master/

# --------------------------------------------------
# Update the 4 params below to run this code
# --------------------------------------------------
dataverse_server = 'https://dev.archaeology.datastations.nl' # no trailing slash
api_key = '28c323a6-b315-4cc4-ba45-b794fe949313'
#dataset_id = 1  # database id of the dataset
persistentId = 'doi:10.5072/DAR/TRFGXV' # doi or hdl of the dataset

# --------------------------------------------------
# Prepare "file"
# --------------------------------------------------
file_content = 'content: %s' % datetime.now()
files = {'file': ('sample_file2.txt', file_content)}

# --------------------------------------------------
# Using a "jsonData" parameter
# --------------------------------------------------
params = dict(directoryLabel='bar')

params_as_json_string = json.dumps(params)

payload = dict(jsonData=params_as_json_string)

# # --------------------------------------------------
# # Add file using the Dataset's id
# # --------------------------------------------------
# url_dataset_id = '%s/api/datasets/%s/add?key=%s' % (dataverse_server, dataset_id, api_key)
#
# # -------------------
# # Make the request
# # -------------------
# print '-' * 40
# print 'making request: %s' % url_dataset_id
# r = requests.post(url_dataset_id, data=payload, files=files)
#
# # -------------------
# # Print the response
# # -------------------
# print '-' * 40
# print r.json()
# print r.status_code
#
# --------------------------------------------------
# Add file using the Dataset's persistentId (e.g. doi, hdl, etc)
# --------------------------------------------------
url_persistent_id = '%s/api/datasets/:persistentId/add?persistentId=%s&key=%s' % (dataverse_server, persistentId, api_key)

# -------------------
# Update the file content to avoid a duplicate file error
# -------------------
file_content = 'content2: %s' % datetime.now()
files = {'file': ('sample_file2.txt', file_content)}


# -------------------
# Make the request
# -------------------
print ('-' * 40)
print ('making request: %s' % url_persistent_id)
r = requests.post(url_persistent_id, data=payload, files=files, verify=False)

# -------------------
# Print the response
# -------------------
print ('-' * 40)
print (r.json())
print (r.status_code)
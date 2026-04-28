import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from datetime import datetime
import json

########################## configuration for a draft dataset without files

dataverse_server = 'https://dev.archaeology.datastations.nl'
api_key = '5623d6e3-bc94-40a5-8de0-8ebdf9f58cbc'
persistentId = 'doi:10.5072/DAR/HBGPN5'

####################
print ('-' * 40 + ' preparation: add file foo/bar')

url = '%s/api/datasets/:persistentId/add?persistentId=%s' % (dataverse_server, persistentId)
unique_content = 'content2: %s' % datetime.now()
files = {'file': ('bar', unique_content)}
payload = {"jsonData": json.dumps({"directoryLabel": "foo"})}# conflicting dir
r = requests.post(url, headers={'X-Dataverse-key': api_key}, data=payload, files=files, verify=False)
print (r.status_code)
print (r.json())

####################
print ('-' * 40 + ' preparation: add file x to have a file to change')

###
url = '%s/api/datasets/:persistentId/add?&persistentId=%s' % (dataverse_server, persistentId)
unique_content = 'content2: %s' % datetime.now()
files = {'file': ('x', unique_content)}
payload = {"jsonData": json.dumps({"label": "x"})}
r = requests.post(url, headers={'X-Dataverse-key': api_key}, data=payload, files=files, verify=False)
print (r.status_code)
print (r.json())

file_id = r.json()['data']['files'][0]['dataFile']['id']

####################
print ('-' * 40 + ' file conflicting with existing dir gets sequence number')

###
url = '%s/api/datasets/:persistentId/add?persistentId=%s' % (dataverse_server, persistentId)
unique_content = 'content2: %s' % datetime.now()
files = {'file': ('foo', unique_content)}
payload = payload = {"jsonData": json.dumps({"label": "foo"})}
r = requests.post(url, headers={'X-Dataverse-key': api_key}, data=payload, files=files, verify=False)

print(r.status_code)
print (r.json())
print (r.status_code)

####################
print ('-' * 40 + ' files API:  dir foo/bar conflicts with previously created file foo/bar')

### files API https://guides.dataverse.org/en/latest/api/native-api.html#updating-file-metadata
url = f'{dataverse_server}/api/files/{file_id}/metadata'
files = {'jsonData': (None, '{"directoryLabel": "foo/bar", "label": "files-api.txt"}')}
r = requests.post(url, headers={'X-Dataverse-key': api_key}, files=files, verify=False)

print(r.status_code)
print(r.text)

####################
print ('-' * 40 + ' datasets API: conflicting file name')

### datasets API https://guides.dataverse.org/en/latest/api/native-api.html#update-file-metadata
url = f'{dataverse_server}/api/datasets/:persistentId/files/metadata?key={api_key}&persistentId={persistentId}'
json_content = [{"dataFileId": file_id, "directoryLabel": "foo/bar", "label": "datasets-api.txt"}]
headers = {'X-Dataverse-key': api_key, 'Content-Type': 'application/json'}
r = requests.post(url, headers=headers, json=json_content, verify=False)

print(r.status_code)
print(r.text)

####################
print ('-' * 40 + ' file conflicting with existing file: gets seq nr')

url = '%s/api/datasets/:persistentId/add?persistentId=%s' % (dataverse_server, persistentId)
unique_content = 'content2: %s' % datetime.now()
files = {'file': ('fox', unique_content)}
payload = payload = {"jsonData": json.dumps({"label": "x"})}
r = requests.post(url, headers={'X-Dataverse-key': api_key}, data=payload, files=files, verify=False)

print(r.status_code)
print (r.json())
print (r.status_code)

####################
print ('-' * 40 + ' dir conflicting with existing file: should fail')
# TODO should fail but does not, see new unit test
#   how to make saveAndAddFilesToDataset return an empty (or shorter list)
#   when checkForDuplicateFileNamesFinal detects a directory that duplicates an existing file

url = '%s/api/datasets/:persistentId/add?persistentId=%s' % (dataverse_server, persistentId)
unique_content = 'content2: %s' % datetime.now()
files = {'file': ('foo', unique_content)}
payload = payload = {"jsonData": json.dumps({"label": "dir-conflicts-with-file.txt", "directoryLabel": "foo/bar"})}
r = requests.post(url, headers={'X-Dataverse-key': api_key}, data=payload, files=files, verify=False)

print(r.status_code)
print (r.json())
print (r.status_code)

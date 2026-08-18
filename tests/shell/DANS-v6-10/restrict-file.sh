set -ex

cd ~/git/dans-core-systems/

#grep dev_lifesciences config.yml
start-preprovisioned-box.py -s dev_vocabs dev_dataversenl
# to be able to check migration:
# manually add restrictions and all terms fields with a custom license

#git --git-dir external/dataverse/.git checkout DD-2318-spilt-termsOUAA
mvn -f external/dataverse/pom.xml clean install -DskipTests
deploy.py -e shared_dataverse_payara_dir=payara7 --dataverse-war external/dataverse/target/dataverse dev_dataversenl

## show potential problems caused by the flyway
#vagrant ssh dev_dataversenl -c 'journalctl -u payara | grep ERR | tail -10'
#
## show the flyway script applied the changes
#vagrant ssh dev_dataversenl -c "sudo -u postgres psql dvndb -c \"select * from termsofaccess;select * from termsofuseorlicense;select * from datasetversion;select * from template;select * from filemetadata;\""
#vagrant ssh dev_dataversenl -c "sudo -u postgres psql dvndb -c \"select * from flyway_schema_history;\"" < /dev/null | egrep '(DD|version)'
#
## show how we deployed the flyway script
#grep create-tables external/dataverse/src/main/resources/META-INF/persistence.xml

# the key is only for a dataverse VM which is not publicly accessible, so it is safe to share it here
export API_TOKEN=c87eb8fc-b4e0-4543-a57b-5533834c4b58

# files     10 8 6
# datasets   9 7 5

# &sourceLastUpdateTime=2026-07-19T13:48:15Z
# is the lastUpdateTime from the exported metadata but causes timestamp outdated error
curl -X PUT "https://dev.dataverse.nl/api/datasets/9/editMetadata?replace=true" -H "X-Dataverse-key: $API_TOKEN" -H "Content-Type: application/json" \
     -d '{ "fields": [ { "typeName": "subtitle", "value": "More stars for testing" }] }'

curl -H "X-Dataverse-key:$API_TOKEN" -X PUT "https://dev.dataverse.nl/api/files/10/restrict" -H "Content-Type: application/json" \
     -d '{"restrict": true, "enableAccessRequest":true, "termsOfAccess": "Reason for the restricted access"}'
curl -H "X-Dataverse-key: $API_TOKEN" -X POST "https://dev.dataverse.nl/api/datasets/9/actions/:publish?type=minor"

curl -H "X-Dataverse-key:$API_TOKEN" -X PUT "https://dev.dataverse.nl/api/datasets/9/access" -H "Content-Type: application/json" \
     -d '{ "customTermsOfAccess": { "fileAccessRequest": true, "termsOfAccess": "Your changed terms of access for restricted files" } }'
sleep 5
curl -H "X-Dataverse-key: $API_TOKEN" -X POST "https://dev.dataverse.nl/api/datasets/9/actions/:publish?type=updatecurrent"

curl -X PUT "https://dev.dataverse.nl/api/datasets/5/license" -H "X-Dataverse-key: $API_TOKEN" -H "Content-Type: application/json" -d '{ "name": "CC BY 4.0" }'
curl -X PUT "https://dev.dataverse.nl/api/datasets/5/license" -H "X-Dataverse-key: $API_TOKEN" -H "Content-Type: application/json" \
     -d '{ "customTerms": { "termsOfUse": "Your terms of use", "restrictions": "Your restrictions" } }'

# dataverses 1(root) 2(dans) 3(general) 4(testv610)

curl -X POST "https://dev.dataverse.nl/api/dataverses/4/templates" -H "X-Dataverse-key: $API_TOKEN" -H "Content-Type: application/json" \
--upload-file external/dataverse/tests/shell/DANS-v6-10/template-CCBY.json
# NewTemplateDTO does not allow to set the termsOfUse and termsOfUseAndLicense fields
# that functionality is web-ui only


CREATE TABLE IF NOT EXISTS TERMSOFACCESS (
    ID  SERIAL NOT NULL,
    AVAILABILITYSTATUS TEXT,
    CONTACTFORACCESS TEXT,
    CONFIDENTIALITYDECLARATION TEXT,
    DATAACCESSPLACE TEXT,
    ORIGINALARCHIVE TEXT,
    SIZEOFCOLLECTION TEXT,
    STUDYCOMPLETION TEXT,
    TERMSOFACCESS TEXT,
    FILEACCESSREQUEST BOOLEAN,
    PRIMARY KEY (ID)
);

CREATE TABLE IF NOT EXISTS TERMSOFUSEORLICENSE (
    ID  SERIAL NOT NULL,
    CITATIONREQUIREMENTS TEXT,
    CONDITIONS TEXT,
    CONFIDENTIALITYDECLARATION TEXT,
    DEPOSITORREQUIREMENTS TEXT,
    DISCLAIMER TEXT,
    FILEACCESSREQUEST BOOLEAN,
    LICENSE_ID BIGINT,
    RESTRICTIONS TEXT,
    SPECIALPERMISSIONS TEXT,
    TERMSOFUSE TEXT,
    PRIMARY KEY (ID)
);

INSERT INTO termsofuseorlicense (id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse)
SELECT id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse
FROM termsofuseandaccess WHERE id NOT IN (SELECT id FROM termsofuseorlicense);

INSERT INTO termsofaccess (id, availabilitystatus, contactforaccess, confidentialitydeclaration, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest)
SELECT id, availabilitystatus, contactforaccess, confidentialitydeclaration, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest
FROM termsofuseandaccess WHERE id NOT IN (SELECT id FROM termsofaccess);

ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT;
ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS  default_termsofuseorlicense_id BIGINT;

ALTER TABLE template ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT;
ALTER TABLE template ADD COLUMN IF NOT EXISTS  default_termsofuseorlicense_id BIGINT;

ALTER TABLE filemetadata ADD COLUMN IF NOT EXISTS termsofuseorlicense_id BIGINT;

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_termsofuseorlicense_license_id') THEN
            ALTER TABLE termsofuseorlicense ADD CONSTRAINT fk_termsofuseorlicense_license_id foreign key (license_id) REFERENCES license(id);
        END IF;

        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='datasetversion' and column_name='termsofuseandaccess_id') THEN
            UPDATE datasetversion SET termsofaccess_id = termsofuseandaccess_id, default_termsofuseorlicense_id = termsofuseandaccess_id;
            ALTER TABLE datasetversion DROP COLUMN termsofuseandaccess_id;
            ALTER TABLE datasetversion ALTER COLUMN termsofaccess_id SET NOT NULL;
        END IF;

        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='template' and column_name='termsofuseandaccess_id') THEN
            UPDATE template SET termsofaccess_id = termsofuseandaccess_id, default_termsofuseorlicense_id = termsofuseandaccess_id;
            ALTER TABLE template DROP COLUMN termsofuseandaccess_id;
            ALTER TABLE template ALTER COLUMN termsofaccess_id SET NOT NULL;
        END IF;
    END
$$;
--------------------------------------------------------------------------
-- Perhaps less code breaks with the view, no updates are possible though.
--------------------------------------------------------------------------
DROP TABLE IF EXISTS termsofuseandaccess;
CREATE VIEW termsofuseandaccess AS
SELECT
 l.id,
 a.availabilitystatus,
 l.citationrequirements,
 l.conditions,
 l.confidentialitydeclaration,
 a.contactforaccess,
 a.dataaccessplace,
 l.depositorrequirements,
 l.disclaimer,
 l.fileaccessrequest,
 a.originalarchive,
 l.restrictions,
 a.sizeofcollection,
 l.specialpermissions,
 a.studycompletion,
 a.termsofaccess,
 l.termsofuse,
 l.license_id
FROM termsofaccess a
LEFT JOIN termsofuseorlicense l ON a.id = l.id;
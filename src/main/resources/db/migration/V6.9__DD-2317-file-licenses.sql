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
    LICENSE_ID BIGINT REFERENCES LICENSE(ID),
    RESTRICTIONS TEXT,
    SPECIALPERMISSIONS TEXT,
    TERMSOFUSE TEXT,
    PRIMARY KEY (ID)
);

INSERT INTO termsofuseorlicense (id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse)
SELECT id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse
FROM termsofuseandaccess a WHERE NOT EXISTS (select 1 FROM termsofuseorlicense t WHERE t.id = a.id);

INSERT INTO termsofaccess (id, availabilitystatus, contactforaccess, confidentialitydeclaration, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest)
SELECT id, availabilitystatus, contactforaccess, confidentialitydeclaration, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest
FROM termsofuseandaccess a WHERE NOT EXISTS (select 1 FROM termsofaccess t WHERE t.id = a.id);

ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT REFERENCES termsofaccess(id);
ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS default_termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);

ALTER TABLE template ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT REFERENCES termsofaccess(id);
ALTER TABLE template ADD COLUMN IF NOT EXISTS termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);

ALTER TABLE filemetadata ADD COLUMN IF NOT EXISTS termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='datasetversion' and column_name='termsofuseandaccess_id') THEN
        UPDATE datasetversion SET termsofaccess_id = termsofuseandaccess_id, default_termsofuseorlicense_id = termsofuseandaccess_id;
        ALTER TABLE datasetversion DROP COLUMN termsofuseandaccess_id;
    ELSE
        RAISE NOTICE 'Column termsofuseandaccess_id already dropped.';
    END IF;
    ALTER TABLE datasetversion ALTER COLUMN termsofaccess_id SET NOT NULL;
    ALTER TABLE datasetversion ALTER COLUMN default_termsofuseorlicense_id SET NOT NULL;

    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='template' and column_name='termsofuseandaccess_id') THEN
        UPDATE template SET termsofaccess_id = termsofuseandaccess_id, termsofuseorlicense_id = termsofuseandaccess_id;
        ALTER TABLE template DROP COLUMN termsofuseandaccess_id;
    ELSE
        RAISE NOTICE 'Column termsofuseandaccess_id already dropped';
    END IF;
    ALTER TABLE template ALTER COLUMN termsofaccess_id SET NOT NULL;
    ALTER TABLE template ALTER COLUMN termsofuseorlicense_id SET NOT NULL;

    -- assumption: if one row is migrated, all are migrated
    IF EXISTS (SELECT 1 FROM termsofuseandaccess) AND
       EXISTS (SELECT 1 FROM termsofuseorlicense) AND
       NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname='termsofuseandaccess')
    THEN
        DROP TABLE IF EXISTS termsofuseandaccess;
    ELSE
        RAISE NOTICE 'Not dropping termsofuseandaccess table, it is a view or not yet migrated.';
    END IF;
END
$$;

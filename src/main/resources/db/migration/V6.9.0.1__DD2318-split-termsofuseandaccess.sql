DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='termsofaccess') THEN
        CREATE TABLE TERMSOFACCESS (
                               ID  SERIAL NOT NULL,
                               AVAILABILITYSTATUS TEXT,
                               CONTACTFORACCESS TEXT,
                               DATAACCESSPLACE TEXT,
                               ORIGINALARCHIVE TEXT,
                               SIZEOFCOLLECTION TEXT,
                               STUDYCOMPLETION TEXT,
                               TERMSOFACCESS TEXT,
                               FILEACCESSREQUEST BOOLEAN,
                               PRIMARY KEY (ID)
        );

        CREATE TABLE TERMSOFUSEORLICENSE (
                                     ID  SERIAL NOT NULL,
                                     CITATIONREQUIREMENTS TEXT,
                                     CONDITIONS TEXT,
                                     CONFIDENTIALITYDECLARATION TEXT,
                                     DEPOSITORREQUIREMENTS TEXT,
                                     DISCLAIMER TEXT,
                                     LICENSE_ID BIGINT REFERENCES LICENSE(ID),
                                     RESTRICTIONS TEXT,
                                     SPECIALPERMISSIONS TEXT,
                                     TERMSOFUSE TEXT,
                                     PRIMARY KEY (ID)
        );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM termsofuseorlicense) THEN
        INSERT INTO termsofuseorlicense (id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse)
        SELECT id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, fileaccessrequest, license_id, restrictions, specialpermissions, termsofuse
        FROM termsofuseandaccess;

        INSERT INTO termsofaccess (id, availabilitystatus, contactforaccess, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest)
        SELECT id, availabilitystatus, contactforaccess, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest
        FROM termsofuseandaccess;

        ALTER TABLE datasetversion ADD COLUMN termsofaccess_id BIGINT REFERENCES termsofaccess(id);
        ALTER TABLE datasetversion ADD COLUMN default_termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);
        ALTER TABLE template ADD COLUMN termsofaccess_id BIGINT REFERENCES termsofaccess(id);
        ALTER TABLE template ADD COLUMN termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);
        ALTER TABLE filemetadata ADD COLUMN termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);

        UPDATE datasetversion SET termsofaccess_id = termsofuseandaccess_id, default_termsofuseorlicense_id = termsofuseandaccess_id;
        UPDATE template SET termsofaccess_id = termsofuseandaccess_id, termsofuseorlicense_id = termsofuseandaccess_id;

        ALTER TABLE datasetversion DROP COLUMN termsofuseandaccess_id;
        ALTER TABLE template DROP COLUMN termsofuseandaccess_id;

        ALTER TABLE datasetversion ALTER COLUMN termsofaccess_id SET NOT NULL;
        ALTER TABLE datasetversion ALTER COLUMN default_termsofuseorlicense_id SET NOT NULL;
        ALTER TABLE template ALTER COLUMN termsofaccess_id SET NOT NULL;
        ALTER TABLE template ALTER COLUMN termsofuseorlicense_id SET NOT NULL;

        DROP TABLE termsofuseandaccess;

        GRANT SELECT, INSERT, UPDATE, DELETE ON termsofaccess TO dvnuser;
        GRANT SELECT, INSERT, UPDATE, DELETE ON termsofuseorlicense TO dvnuser;
        GRANT USAGE, SELECT ON SEQUENCE termsofaccess_id_seq TO dvnuser;
        GRANT USAGE, SELECT ON SEQUENCE termsofuseorlicense_id_seq TO dvnuser;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM termsofaccess) THEN
        PERFORM setval('termsofaccess_id_seq', MAX(id)) FROM termsofaccess;
        PERFORM setval('termsofuseorlicense_id_seq', MAX(id)) FROM termsofuseorlicense;
    END IF;
END
$$;

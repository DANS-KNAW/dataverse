CREATE TABLE IF NOT EXISTS termsofaccess (
                       id  SERIAL NOT NULL,
                       availabilitystatus TEXT,
                       contactforaccess TEXT,
                       dataaccessplace TEXT,
                       originalarchive TEXT,
                       sizeofcollection TEXT,
                       studycompletion TEXT,
                       termsofaccess TEXT,
                       fileaccessrequest BOOLEAN,
                       PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS termsofuseorlicense (
                             id  SERIAL NOT NULL,
                             citationrequirements TEXT,
                             conditions TEXT,
                             confidentialitydeclaration TEXT,
                             depositorrequirements TEXT,
                             disclaimer TEXT,
                             license_id BIGINT REFERENCES license(id),
                             restrictions TEXT,
                             specialpermissions TEXT,
                             termsofuse TEXT,
                             PRIMARY KEY (id)
);

DO $$
BEGIN
    ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT REFERENCES termsofaccess(id);
    ALTER TABLE datasetversion ADD COLUMN IF NOT EXISTS default_termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);
    ALTER TABLE template ADD COLUMN IF NOT EXISTS termsofaccess_id BIGINT REFERENCES termsofaccess(id);
    ALTER TABLE template ADD COLUMN IF NOT EXISTS termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);
    ALTER TABLE filemetadata ADD COLUMN IF NOT EXISTS termsofuseorlicense_id BIGINT REFERENCES termsofuseorlicense(id);

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='termsofuseandaccess') THEN
        INSERT INTO termsofuseorlicense (id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, license_id, restrictions, specialpermissions, termsofuse)
        SELECT id, citationrequirements, conditions, confidentialitydeclaration, depositorrequirements, disclaimer, license_id, restrictions, specialpermissions, termsofuse
        FROM termsofuseandaccess
        ON CONFLICT (id) DO NOTHING;

        INSERT INTO termsofaccess (id, availabilitystatus, contactforaccess, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest)
        SELECT id, availabilitystatus, contactforaccess, dataaccessplace, originalarchive, sizeofcollection, studycompletion, termsofaccess, fileaccessrequest
        FROM termsofuseandaccess
        ON CONFLICT (id) DO NOTHING;

    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='datasetversion' AND column_name='termsofuseandaccess_id') THEN
        UPDATE datasetversion
        SET termsofaccess_id = termsofuseandaccess_id,
            default_termsofuseorlicense_id = termsofuseandaccess_id
        WHERE termsofaccess_id IS NULL OR default_termsofuseorlicense_id IS NULL;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='template' AND column_name='termsofuseandaccess_id') THEN
        UPDATE template
        SET termsofaccess_id = termsofuseandaccess_id,
            termsofuseorlicense_id = termsofuseandaccess_id
        WHERE termsofaccess_id IS NULL OR termsofuseorlicense_id IS NULL;
    END IF;

    ALTER TABLE datasetversion DROP COLUMN IF EXISTS termsofuseandaccess_id;
    ALTER TABLE template DROP COLUMN IF EXISTS termsofuseandaccess_id;

    DROP TABLE IF EXISTS termsofuseandaccess;

    GRANT SELECT, INSERT, UPDATE, DELETE ON termsofaccess TO dvnuser;
    GRANT SELECT, INSERT, UPDATE, DELETE ON termsofuseorlicense TO dvnuser;
    GRANT USAGE, SELECT ON SEQUENCE termsofaccess_id_seq TO dvnuser;
    GRANT USAGE, SELECT ON SEQUENCE termsofuseorlicense_id_seq TO dvnuser;
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

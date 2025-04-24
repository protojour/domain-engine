ALTER TABLE m6mreg.domain ADD COLUMN subdomain integer NOT NULL DEFAULT 0;
ALTER TABLE m6mreg.domain DROP CONSTRAINT domain_uid_key;
CREATE UNIQUE INDEX domain_uid_subdomain_key ON m6mreg.domain USING btree (uid, subdomain);

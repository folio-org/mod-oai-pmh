CREATE SCHEMA oaitest_mod_inventory_storage;

CREATE TABLE oaitest_mod_inventory_storage.instance (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL,
    creation_date timestamp without time zone,
    created_by text,
    instancestatusid uuid,
    modeofissuanceid uuid,
    instancetypeid uuid
);
CREATE SCHEMA oaitest_mod_source_record_storage;

CREATE SCHEMA oaitest_mod_oai_pmh;

CREATE TYPE oaitest_mod_source_record_storage.record_type AS ENUM
    ('MARC_BIB', 'MARC_AUTHORITY', 'MARC_HOLDING', 'EDIFACT');

CREATE TYPE oaitest_mod_source_record_storage.record_state AS ENUM
    ('ACTUAL', 'DRAFT', 'OLD', 'DELETED');


CREATE TABLE oaitest_mod_source_record_storage.records_lb (
    id uuid NOT NULL,
    snapshot_id uuid NOT NULL,
    matched_id uuid NOT NULL,
    generation integer NOT NULL,
    record_type oaitest_mod_source_record_storage.record_type NOT NULL,
    external_id uuid,
    state oaitest_mod_source_record_storage.record_state NOT NULL,
    leader_record_status character(1),
    "order" integer,
    suppress_discovery boolean DEFAULT false,
    created_by_user_id uuid,
    created_date timestamp with time zone,
    updated_by_user_id uuid,
    updated_date timestamp with time zone,
    external_hrid text
);

CREATE TABLE oaitest_mod_source_record_storage.marc_records_lb (
    id uuid NOT NULL,
    content jsonb NOT NULL
);

CREATE TABLE oaitest_mod_inventory_storage.holdings_record (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL,
    creation_date timestamp without time zone,
    created_by text,
    instanceid uuid,
    permanentlocationid uuid,
    temporarylocationid uuid,
    effectivelocationid uuid,
    holdingstypeid uuid,
    callnumbertypeid uuid,
    illpolicyid uuid,
    sourceid uuid
);

CREATE TABLE oaitest_mod_inventory_storage.item (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL,
    creation_date timestamp without time zone,
    created_by text,
    holdingsrecordid uuid,
    permanentloantypeid uuid,
    temporaryloantypeid uuid,
    materialtypeid uuid,
    permanentlocationid uuid,
    temporarylocationid uuid,
    effectivelocationid uuid
);

CREATE TABLE oaitest_mod_inventory_storage.audit_instance (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL
);

CREATE TABLE oaitest_mod_inventory_storage.audit_holdings_record (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL
);

CREATE TABLE oaitest_mod_inventory_storage.audit_item (
    id uuid NOT NULL,
    jsonb jsonb NOT NULL
);

CREATE OR REPLACE FUNCTION oaitest_mod_inventory_storage.strtotimestamp(
	text)
    RETURNS timestamp with time zone
    LANGUAGE 'sql'
    COST 100
    IMMUTABLE STRICT PARALLEL UNSAFE
AS $BODY$
SELECT $1::timestamptz
$BODY$;

CREATE TABLE oaitest_mod_oai_pmh.request_metadata_lb (
    request_id uuid NOT NULL,
    last_updated_date timestamp with time zone NOT NULL,
    stream_ended boolean DEFAULT true NOT NULL,
    returned_instances_counter integer DEFAULT 0,
    skipped_instances_counter integer DEFAULT 0,
    failed_instances_counter integer DEFAULT 0,
    suppressed_instances_counter integer DEFAULT 0,
    downloaded_and_saved_instances_counter integer DEFAULT 0,
    failed_to_save_instances_counter integer DEFAULT 0
);

CREATE TABLE oaitest_mod_oai_pmh.rmb_internal (
    id integer NOT NULL,
    jsonb jsonb NOT NULL
);

CREATE ROLE oaitest_mod_oai_pmh WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;

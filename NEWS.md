## 3.15.0 - Unreleased

## 3.14.4 (Released)

This release includes bug fixes (adding missing interface).

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.14.3...v3.14.4)

## 3.14.3 (Released)

This release includes bug fixes.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.14.2...v3.14.3)

### Bug fixes
* [MODOAIPMH-584](https://folio-org.atlassian.net/browse/MODOAIPMH-584) Character encoding inconsistent for non-ASCII location names in OAI-PMH 952 holdings
* [MODOAIPMH-567](https://folio-org.atlassian.net/browse/MODOAIPMH-567) GetRecord unexpectedly returns "invalidRecordContent" error for deleted MARC Instance

## 3.14.2 (Released)

This release includes fix missing interface dependencies and minor improvements.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.14.1...v3.14.2)

### Bug fixes
* [MODOAIPMH-585](https://folio-org.atlassian.net/browse/MODOAIPMH-585) Missing interface dependencies in module descriptor

## 3.14.1 (Released)

This release includes only aws sdk dependency update

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.14.0...v3.14.1)

## 3.14.0 (Released)

This release includes dependency updates and minor fixes

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.13.0...v3.14.0)

### Stories
* [MODOAIPMH-556](https://folio-org.atlassian.net/browse/MODOAIPMH-556) Improve clarity of the error message

### Technical tasks
* [MODOAIPMH-576](https://folio-org.atlassian.net/browse/MODOAIPMH-576) Update source-storage-records to v3.3 and source-storage-source-records to v3.2 with module permission renaming
* [MODOAIPMH-575](https://folio-org.atlassian.net/browse/MODOAIPMH-575) API version update
* [FOLIO-4087](https://folio-org.atlassian.net/browse/FOLIO-4087) RMB & Spring upgrades (all modules)

### Bug fixes
* [MODOAIPMH-578](https://folio-org.atlassian.net/browse/MODOAIPMH-578) OAI-PMH GetRecord call does not return MARC records after token was expired and refreshed
* [MODOAIPMH-550](https://folio-org.atlassian.net/browse/MODOAIPMH-550) Harvesting FOLIO Instances with oai_dc metadataPrefix returns "text" in <dc:type> regardless the Instance resource type

## 3.13.0 (Released)

This release includes dependency updates and minor fixes

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.8...v3.13.0)

### Technical tasks
* [MODOAIPMH-564](https://folio-org.atlassian.net/browse/MODOAIPMH-564) mod-oai-pmh: Vertx 4.5.* upgrade
* [MODOAIPMH-559](https://folio-org.atlassian.net/browse/MODOAIPMH-559) mod-oai-pmh: Upgrade RAML Module Builder
* [MODOAIPMH-523](https://folio-org.atlassian.net/browse/MODOAIPMH-523) Add holdings ILL Policy to the fields exported as part of the withholdings metadatPrefix

## 3.12.8 (Released)

This release includes bug fixes for member tenant harvesting

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.7...v3.12.8)

### Bug fixes
* [MODOAIPMH-549](https://issues.folio.org/browse/MODOAIPMH-549) ECS: Inconsistent response for verb=GetRecord&metadataPrefix=marc21_withholdings for instances shared from member tenant

## 3.12.7 (Released)

This release includes folio-s3-client update.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.6...v3.12.7)

## 3.12.6 (Released)

This release includes bug fixes for suppressed from discovery and missing columns in request_metadata_lb.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.5...v3.12.6)

### Bug fixes
* [MODOAIPMH-546](https://issues.folio.org/browse/MODOAIPMH-546) ECS: ListRecords returns suppressed from discovery Shared MARC Instance with flag t=0 in 999 and 856 fields
* [MODOAIPMH-545](https://issues.folio.org/browse/MODOAIPMH-545) Missing columns in request_metadata_lb in Poppy upgraded environments

## 3.12.5 (Released)

This release includes folio-s3-client update.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.4...v3.12.5)

## 3.12.4 (Released)

This release includes folio-s3-client update.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.3...v3.12.4)

## 3.12.3 (Released)

This release includes folio-s3-client update.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.2...v3.12.3)

## 3.12.2 (Released)

This release includes dependency update.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.1...v3.12.2)

## 3.12.1 (Released)

This release includes fixes for vulnerabilities and memory leaks.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.12.0...v3.12.1)

### Bug fixes
* [MODOAIPMH-543](https://issues.folio.org/browse/MODOAIPMH-543) generate-marc-utils 1.7.0 fixing json-smart stack overflow


## 3.12.0 (Released)

This release includes bug fixes, performance improvements, code refactoring and technical tasks.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.11.0...v3.12.0)

### Stories
* [MODOAIPMH-515](https://issues.folio.org/browse/MODOAIPMH-515) Export harvest logs into csv format
* [MODOAIPMH-514](https://issues.folio.org/browse/MODOAIPMH-514) Purge error logs
* [MODOAIPMH-513](https://issues.folio.org/browse/MODOAIPMH-513) Avoid skipping entire batch when 500 Internal Server Error from inventory
* [MODOAIPMH-492](https://issues.folio.org/browse/MODOAIPMH-492) Re-work asynchronous code for harvesting
* [MODOAIPMH-477](https://issues.folio.org/browse/MODOAIPMH-477) Adjust 856 mappings
* [MODOAIPMH-433](https://issues.folio.org/browse/MODOAIPMH-433) Unable to build on ARM
* [MODOAIPMH-322](https://issues.folio.org/browse/MODOAIPMH-322) Implement performance improvement of harvesting with marc21 prefix

### Technical tasks
* [MODOAIPMH-534](https://issues.folio.org/browse/MODOAIPMH-534) Handle CONSORTIUM-FOLIO instances in the harvest
* [MODOAIPMH-533](https://issues.folio.org/browse/MODOAIPMH-533) Add missing electronic access relationship value to default rules
* [MODOAIPMH-525](https://issues.folio.org/browse/MODOAIPMH-525) Update to Java 17 mod-oai-pmh
* [MODOAIPMH-524](https://issues.folio.org/browse/MODOAIPMH-524) SRS-client with "shared" MARC records support
* [MODOAIPMH-516](https://issues.folio.org/browse/MODOAIPMH-516) Resumption Token Extension
* [MODOAIPMH-491](https://issues.folio.org/browse/MODOAIPMH-491) Implement query builder for the new approach of OAI-PMH
* [MODOAIPMH-490](https://issues.folio.org/browse/MODOAIPMH-490) Inventory-client to views mechanism replacement
* [MODOAIPMH-457](https://issues.folio.org/browse/MODOAIPMH-457) PoC for moving to RMB approach

### Bug fixes
* [MODOAIPMH-530](https://issues.folio.org/browse/MODOAIPMH-530) OAI-PMH: Incorrect mapping rules for "Linking ISSN" identifier
* [MODOAIPMH-519](https://issues.folio.org/browse/MODOAIPMH-519) SRS records which are not marked as "deleted" are omitted in response with "Deleted records support" set to "NO"
* [MODOAIPMH-507](https://issues.folio.org/browse/MODOAIPMH-507) 856 field subfield "t" is not returned in response for Instance with electronic access in some cases
* [MODOAIPMH-499](https://issues.folio.org/browse/MODOAIPMH-499) mod-oai-pmh returns 500 error when depended module reboots
* [MODOAIPMH-480](https://issues.folio.org/browse/MODOAIPMH-480) 952 field subfield "n" is not returned in response for holdings without items
* [MODOAIPMH-458](https://issues.folio.org/browse/MODOAIPMH-458) Duplicated "t" subfield in 856 field
* [MODOAIPMH-442](https://issues.folio.org/browse/MODOAIPMH-442) bad data in call number type field returns 500 error

## 3.11.0 (Released)

This release contains minor improvements, bug fixes and adding inventory as records source

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.10.0...v3.11.0)

### Stories
* [MODOAIPMH-448](https://issues.folio.org/browse/MODOAIPMH-448) Set default settings for OAI-PMH in FOLIO
* [MODOAIPMH-434](https://issues.folio.org/browse/MODOAIPMH-434) Correct verbiage of error messaged
* [MODOAIPMH-224](https://issues.folio.org/browse/MODOAIPMH-224) Retrieve records from inventory and SRS for GetRecord response - MARC format
* [MODOAIPMH-138](https://issues.folio.org/browse/MODOAIPMH-138) Retrieve records from inventory and SRS for ListRecords response - MARC format

### Technical tasks
* [MODOAIPMH-475](https://issues.folio.org/browse/MODOAIPMH-475) Increase max event loop execute time
* [MODOAIPMH-463](https://issues.folio.org/browse/MODOAIPMH-463) Align the module with API breaking change
* [MODOAIPMH-453](https://issues.folio.org/browse/MODOAIPMH-453) Logging improvement - Configuration
* [MODOAIPMH-422](https://issues.folio.org/browse/MODOAIPMH-422) Make DATABASE_FETCHING_CHUNK_SIZE configurable in the Configuration
* [MODOAIPMH-395](https://issues.folio.org/browse/MODOAIPMH-395) Logging improvement

### Bug fixes
* [MODOAIPMH-473](https://issues.folio.org/browse/MODOAIPMH-473) "856" field is omitted in responce for Electronic access relationship type created by user
* [MODOAIPMH-449](https://issues.folio.org/browse/MODOAIPMH-449) OAI-PMH exposes records as deleted even though their leader 05 is set to 's'

## 3.10.0 (Released)

This release contains minor improvements

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.9.0...v3.10.0)

### Technical tasks
* [MODOAIPMH-446](https://issues.folio.org/browse/MODOAIPMH-446) mod-oai-pmh: Upgrade RAML Module Builder
* [MODOAIPMH-440](https://issues.folio.org/browse/MODOAIPMH-440) Support instance-storage 9.0 interface
* [MODOAIPMH-437](https://issues.folio.org/browse/MODOAIPMH-437) Add personal data disclosure form

## 3.9.0 (Released)

This release contains minor improvements and bug fixes

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.8.0...v3.9.0)

### Stories
* [MODOAIPMH-318](https://issues.folio.org/browse/MODOAIPMH-318) Add Item Loan type to the fields exported as part of the withholdings metadatPrefix

### Technical tasks
* [MODOAIPMH-425](https://issues.folio.org/browse/MODOAIPMH-425) RMB v34 upgrade - Morning Glory 2022 R2 module release

### Bug fixes
* [MODOAIPMH-426](https://issues.folio.org/browse/MODOAIPMH-426) OAI-PMH requests made with the verb GetRecord and metadataPrefix set to marc21_withholdings do not return holdings and items data
* [MODOAIPMH-421](https://issues.folio.org/browse/MODOAIPMH-421) Issues with saving instances to the database

## 3.8.0 (Released)

This release contains stability improvements and Statistics API for harvesting monitoring

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.7.1...v3.8.0)

### Stories

* [MODOAIPMH-420](https://issues.folio.org/browse/MODOAIPMH-420) Save UUIDs of failed instances
* [MODOAIPMH-419](https://issues.folio.org/browse/MODOAIPMH-419) Harvesting statistics API: stored instances statistics
* [MODOAIPMH-412](https://issues.folio.org/browse/MODOAIPMH-412) API for harvesting statistics
* [MODOAIPMH-408](https://issues.folio.org/browse/MODOAIPMH-408) mod-oai-pmh instances statistics

### Bug fixes

* [MODOAIPMH-418](https://issues.folio.org/browse/MODOAIPMH-418) Changes to holdings or items are not triggering harvesting records with marc21_withholdings
* [MODOAIPMH-417](https://issues.folio.org/browse/MODOAIPMH-417) Handle DB timeouts on mod-oai-pmh

## 3.7.1 (Released)

This release contains upgrade version of RMB to 33.0.2, Vert.x to 4.1.0, improvement of permissions usage

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.7.0...v3.7.1)

### Technical tasks
* [MODOAIPMH-393](https://issues.folio.org/browse/MODOAIPMH-393) Move health test from Jenkins file to integration test

### Bug fixes

* [MODOAIPMH-399](https://issues.folio.org/browse/MODOAIPMH-399) Update to log4j, RMB, Vert.x, postgresql
* [MODOAIPMH-398](https://issues.folio.org/browse/MODOAIPMH-398) Undefined permission 'oai-pmh.sets.item.collection.get'

## 3.7.0 (Released)

This release contains database load and some minor improvements, error fixes and handlers

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.6.2...v3.7.0)

### Bug fixes

* [MODOAIPMH-392](https://issues.folio.org/browse/MODOAIPMH-392) - mod-oai-pmh master-branch build failure
* [MODOAIPMH-379](https://issues.folio.org/browse/MODOAIPMH-379) - Unhandled json parse exception when user does not have adequate permissions

### Stories

* [MODOAIPMH-391](https://issues.folio.org/browse/MODOAIPMH-391) - Unexpected character error
* [MODOAIPMH-378](https://issues.folio.org/browse/MODOAIPMH-378) - Change the way of saving instances ids for reducing the database load
* [MODOAIPMH-377](https://issues.folio.org/browse/MODOAIPMH-377) - Implement metrics for mod-oai-pmh

## 3.6.0 (Released)

This release contains memory usage, database population, handling marc21_withholding, resumption token improvements and bug fixes.

### Stories

* [MODOAIPMH-339](https://issues.folio.org/browse/MODOAIPMH-339) Address memory leaks
* [MODOAIPMH-337](https://issues.folio.org/browse/MODOAIPMH-337) The mod-oai-pmh schema is not populated correctly after RMB 32 update
* [MODOAIPMH-328](https://issues.folio.org/browse/MODOAIPMH-328) align dependency versions affected by Inventory's Optimistic Locking
* [MODOAIPMH-283](https://issues.folio.org/browse/MODOAIPMH-283) Provide effective location and effective call number data when item record is not present
* [MODOAIPMH-164](https://issues.folio.org/browse/MODOAIPMH-164) Add expiration date attribute to resumption token

### Bug fixes

* [MODOAIPMH-317](https://issues.folio.org/browse/MODOAIPMH-317) Holdings data not discoverable when item record is suppressed

  [Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v3.5.0...v3.6.0)

## 3.5.0 (Released) 2021-06-08

This release contains an incrementation of SRS interface versions and bugfix for handling marc21 and oai_dc metadata prefixes.

### Stories

* [MODOAIPMH-331](https://issues.folio.org/browse/MODOAIPMH-331) Update SRS interface versions

### Bug fixes

* [MODOAIPMH-327](https://issues.folio.org/browse/MODOAIPMH-327) Resumption token flow doesn't work as expected for marc21 and oai_dc metadata prefixes.
* [MODOAIPMH-320](https://issues.folio.org/browse/MODOAIPMH-320) 0% coverage reported by Sonarcloud for mod-oai-pmh

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.4.2...v3.5.0)

## 3.4.2 (Released) 2021-04-09

This bug fix release has corrections regarding marc21_withholdings metadata prefix handling.

### Bug fixes

* [MODOAIPMH-312](https://issues.folio.org/browse/MODOAIPMH-312) Rerequesting of SRS doesn't work properly.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.4.1...v3.4.2)

## 3.4.1 (Released) 2021-04-07

This release involves bug fixes for marc21_withholdings metadata prefix handling.

### Bug fixes

* [MODOAIPMH-293](https://issues.folio.org/browse/MODOAIPMH-293) Transfer suppressed records with discovery flag is not honored for instances suppressed from discovery.
* [MODOAIPMH-302](https://issues.folio.org/browse/MODOAIPMH-302) Response encoding does not use UTF-8 represenation of Unicode.
* [MODOAIPMH-203](https://issues.folio.org/browse/MODOAIPMH-203) Add index to mod-oai-pmh.instances table.
* [MODOAIPMH-305](https://issues.folio.org/browse/MODOAIPMH-305) <datestamp> element not populated correctly when harvesting with marc21_withholdings prefix.

### Stories

* [MODOAIPMH-310](https://issues.folio.org/browse/MODOAIPMH-310) Stabilize oai-pmh tests for release.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.4.0...v3.4.1)

## 3.4.0 (Released) 2021-03-18

This release involves enhancement and corrections related to marc21_withholdings metadata prefix handling.

### Stories

* [MODOAIPMH-291](https://issues.folio.org/browse/MODOAIPMH-291) Revert RMB 32 update.
* [MODOAIPMH-294](https://issues.folio.org/browse/MODOAIPMH-294) Make resumptiom token reusable.

### Bug fixes

* [MODOAIPMH-300](https://issues.folio.org/browse/MODOAIPMH-300) Not all instances ids are saved for initial harvest.
* [MODOAIPMH-301](https://issues.folio.org/browse/MODOAIPMH-301) Harvest hangs after a few requests.
* [MODOAIPMH-292](https://issues.folio.org/browse/MODOAIPMH-292) Incorrect cursor returned within resumption token.
* [MODOAIPMH-289](https://issues.folio.org/browse/MODOAIPMH-289) HttpClient's aren't being closed properly.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.3.0...v3.4.0)

## 3.3.0 (Released) 2021-01-28

This release includes the bug fixes and improvements regarding marc21_withholdings metadata prefix processing.

* [MODOAIPMH-282](https://issues.folio.org/browse/MODOAIPMH-282) On 7M records first response to the initial requests with marc21_withholdings metadataPrefix takes more than 20 min
* [MODOAIPMH-284](https://issues.folio.org/browse/MODOAIPMH-284) Return response immediately after the required number of instances will be loaded instead of waiting for completion of all instances loading.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.7...v3.3.0)

## 3.2.7 (Released) 2021-01-13

This release includes the bug fix for marc21_withholdings metadata prefix request.

* [MODOAIPMH-276](https://issues.folio.org/browse/MODOAIPMH-276) Changes to holdings or items are not triggering harvesting records with marc21_withholdings.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.6...v3.2.7)

## 3.2.6 (Released) 2021-01-09

This release includes the exclusion of RMB 32.0.0 updating due to RMB bugs.

* [MODOAIPMH-278](https://issues.folio.org/browse/MODOAIPMH-278) Revert RMB 32.0.0 due to RMB bugs.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.5...v3.2.6)

## 3.2.5 (Released) 2020-12-30

This release mainly contains bug fixes related to marc21_withholdings request and upgrade of RMB up to 32.0.0 .

* [MODOAIPMH-274](https://issues.folio.org/browse/MODOAIPMH-274) Retrieving loaded instance ids doesn't take the request id into account.
* [MODOAIPMH-273](https://issues.folio.org/browse/MODOAIPMH-273) Missing holdings/item fields in ListRecords response with marc21_whitholdings
* [MODOAIPMH-271](https://issues.folio.org/browse/MODOAIPMH-271) Create a database migration script to enrich new oai-pmh tables.
* [MODOAIPMH-266](https://issues.folio.org/browse/MODOAIPMH-266) Upgrade to RMB 32
* [MODOAIPMH-265](https://issues.folio.org/browse/MODOAIPMH-265) Refactor the dao layer and update the readme with initial-load description
* [MODOAIPMH-259](https://issues.folio.org/browse/MODOAIPMH-259) HTML encoded entities in records make the OAI-PMH requests crash
* [MODOAIPMH-258](https://issues.folio.org/browse/MODOAIPMH-258) Clean data for outdated requests from instance table
* [MODOAIPMH-240](https://issues.folio.org/browse/MODOAIPMH-240) Newest git update introduces build loop
* [MODOAIPMH-123](https://issues.folio.org/browse/MODOAIPMH-123) Datestamp in response doesn't correspond to time granularity
* [MODOAIPMH-71](https://issues.folio.org/browse/MODOAIPMH-71) Handle json parsing exceptions gracefully

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.4...v3.2.5)

## 3.2.4 (Released) 2020-11-12

This release includes bug fixes related to marc21_withholdings metadataPrefix and incorrect number of records being returned from ListIdentifiers request.

* [MODOAIPMH-254](https://issues.folio.org/browse/MODOAIPMH-254) Initial load does not contain resumptionToken
* [MODOAIPMH-250](https://issues.folio.org/browse/MODOAIPMH-250) Invalid number of identifiers are returned when get ListIdentifiers
* [MODOAIPMH-252](https://issues.folio.org/browse/MODOAIPMH-252) Mod OAI_PMH container fail after call

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.3...v3.2.4)

## 3.2.3 (Released)

This release includes updating RMB version up to 31.1.5 which fixing issues with database interaction.

* [MODOAIPMH-256](https://issues.folio.org/browse/MODOAIPMH-256) Update RMB version up to 31.1.5

## 3.2.2 (Released)

This release brings updates of both RMB version up to 31.1.3 and vert.x version up to 3.9.4

* [MODOAIPMH-248](https://issues.folio.org/browse/MODOAIPMH-248) Upgrade RMB to 30.2.9:
  * [RMB-740](https://issues.folio.org/browse/RMB-740) Use FOLIO fork of vertx-sql-client and vertx-pg-client with
    the following two patches
  * [RMB-739](https://issues.folio.org/browse/RMB-739) Make RMB's DB\_CONNECTIONRELEASEDELAY work again, defaults to 60 seconds
  * [FOLIO-2840](https://issues.folio.org/browse/FOLIO-2840) Fix duplicate names causing 'prepared statement "XYZ" already exists'
  * [RMB-738](https://issues.folio.org/browse/RMB-738) Upgrade to Vert.x 3.9.4, most notable fix: RowStream fetch
    can close prematurely the stream https://github.com/eclipse-vertx/vertx-sql-client/issues/778

## 3.2.1 (Released)

This is the bugfix release

* [MODOAIPMH-245](https://issues.folio.org/browse/MODOAIPMH-245) Edge module doesn't return the data, even if mod-oai-pmh does

## 3.2.0 (Released)
* [MODOAIPMH-200](https://issues.folio.org/browse/MODOAIPMH-200) Implement the endpoint for getting list of sets
* [MODOAIPMH-201](https://issues.folio.org/browse/MODOAIPMH-201) Enrich sets endpoints with filtering conditions entites
* [MODOAIPMH-206](https://issues.folio.org/browse/MODOAIPMH-206) POST /oai-pmh/set returns 500 when empty string is passed
* [MODOAIPMH-210](https://issues.folio.org/browse/MODOAIPMH-210) Implement endpoint for getting values of all required filtering condition types.
* [MODOAIPMH-218](https://issues.folio.org/browse/MODOAIPMH-218) Rename sets endpoints
* [MODOAIPMH-220](https://issues.folio.org/browse/MODOAIPMH-220) Perform uniqueness validation for sets endpoints
* [MODOAIPMH-223](https://issues.folio.org/browse/MODOAIPMH-223) "totalRecords" isn't shown in response to GET oai-pmh/sets? request
* [MODOAIPMH-227](https://issues.folio.org/browse/MODOAIPMH-227) Set table constraints don't work properly
* [MODOAIPMH-229](https://issues.folio.org/browse/MODOAIPMH-229) Increase postrgres pool connection size and timeout for module
* [MODOAIPMH-238](https://issues.folio.org/browse/MODOAIPMH-238) MODOAIPMH (mod-oai-pmh) release

## 3.0.4 (Released)

* [MODOAIPMH-107](https://issues.folio.org/browse/MODOAIPMH-107) Suppress holdings and items records from discovery
* [MODOAIPMH-192](https://issues.folio.org/browse/MODOAIPMH-192) Resumption token fails from time to time on big amount of data


## 3.0.3 (Released)
 
 Migrating to SRS v4, bugfixing 

## 3.0.2 (Released)

Migrating to SRS v4, include log4j2.properties 

## 3.0.1 (Released)

Bugfixes 

## Stories
* [MODOAIPMH-167](https://issues.folio.org/browse/MODOAIPMH-167) Empty resumption token
* [MODOAIPMH-168](https://issues.folio.org/browse/MODOAIPMH-168) marc21_withholdings prefix response timeout


## 3.0.0 (Released)

This release includes improvements of the current module functionality and enrichment with new functionality which involves support of a new metadata format marc 21 with holdings and items, update RMB version to 30.0.0, moving request validation from edge module, support of the next settings: discovery suppressed records processing, enabling OAI service, deleted records processing, error processing. Added possibility to manipulate OAI-PMH settings from user interface.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v2.2.0...v2.2.1)

### Stories

* [MODOAIPMH-131](https://issues.folio.org/browse/MODOAIPMH-131) Move request validation and configuration settings logic from edge to mod-oai-pmh
* [MODOAIPMH-102](https://issues.folio.org/browse/MODOAIPMH-102) Form ListRecords response enriched with holdings/items fields
* [MODOAIPMH-68](https://issues.folio.org/browse/MODOAIPMH-68) Suppress instance records from discovery
* [MODOAIPMH-141](https://issues.folio.org/browse/MODOAIPMH-141) Add marc21_withholdings to ListMetadataFormats
* [MODOAIPMH-106](https://issues.folio.org/browse/MODOAIPMH-106) Associate OAI-PMH level errors with 200 HTTP status
* [MODOAIPMH-137](https://issues.folio.org/browse/MODOAIPMH-137) Update to RMB v30
* [MODOAIPMH-109](https://issues.folio.org/browse/MODOAIPMH-109) Reflect holdings and items deletion in OAI-PMH response
* [MODOAIPMH-114](https://issues.folio.org/browse/MODOAIPMH-114) Add the suppressDiscovery flag into OAI-PMH feed
* [MODOAIPMH-110](https://issues.folio.org/browse/MODOAIPMH-110) Integrate front-end with back-end to manipulate OAI-PMH settings
* [MODOAIPMH-100](https://issues.folio.org/browse/MODOAIPMH-100) Make OAI-PMH settings access-able from back-end
* [MODOAIPMH-115](https://issues.folio.org/browse/MODOAIPMH-115) Create API to read from new inventory-storage views
* [MODOAIPMH-108](https://issues.folio.org/browse/MODOAIPMH-108) Make deleted instance records support configurable
* [MODOAIPMH-103](https://issues.folio.org/browse/MODOAIPMH-103) Implement "Enable OAI service" setting
* [MODOAIPMH-126](https://issues.folio.org/browse/MODOAIPMH-126) Add suppressed removed records as 'deleted' to the feed
* [MODOAIPMH-124](https://issues.folio.org/browse/MODOAIPMH-124) Add the suppressDiscovery flag into OAI-PMH feed for oai_dc
* [MODOAIPMH-117](https://issues.folio.org/browse/MODOAIPMH-117) Request holdings and items fields from inventory
* [MODOAIPMH-111](https://issues.folio.org/browse/MODOAIPMH-111) Perform validation for OAI-PMH settings
* [MODOAIPMH-69](https://issues.folio.org/browse/MODOAIPMH-69) Change record identifier to use the UUID of the instance record
* [MODOAIPMH-97](https://issues.folio.org/browse/MODOAIPMH-97) Implement UI for OAI-PMH settings

### Bug Fixes

* [MODOAIPMH-121](https://issues.folio.org/browse/MODOAIPMH-121) Fix possibility to complete requests chain with resumptionToken
* [MODOAIPMH-112](https://issues.folio.org/browse/MODOAIPMH-112) Fix possibility to complete requests chain without time borders
* [MODOAIPMH-100](https://issues.folio.org/browse/MODOAIPMH-100) Support configurations management from UI & change the way of working with configurations & splitting configuration into groups.

## 2.1.0 (Released)

This release contains only update to RMB v29.3.0

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v2.0.0...v2.1.0)

* [MODOAIPMH-92](https://issues.folio.org/browse/MODOAIPMH-92) Update RMB from 26.1.2 to 29.3.0 version


## 2.0.0 (Released)
This is a bugfix release to be included in Edelweiss (Q4/2019)

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v1.2.1...v2.0.0)

### Bug Fixes

* [MODOAIPMH-86](https://issues.folio.org/browse/MODOAIPMH-86) Must support "Accept: text/xml"

## 1.2.1 (Released)
This release was to tune and improve environment settings.

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v1.2.0...v1.2.1)

### Stories
* [MODOAIPMH-80](https://issues.folio.org/browse/MODOAIPMH-80) Use JVM features to manage container memory

## 1.2.0 (Released)
Handle breaking changes in SRS and as a result update interface dependencies to avoid breaking mod-oai-pmh.
Add Launch descriptor settings

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v1.1.0...v1.2.0)

### Stories
* [MODOAIPMH-76](https://issues.folio.org/browse/MODOAIPMH-76) Handle breaking changes in SRS
* [FOLIO-2235](https://issues.folio.org/browse/FOLIO-2235) Add LaunchDescriptor settings to each backend non-core module repository

## 1.1.0 (Released)
Bug fix related to SRS records not showing up, and upgrading instance interface dependency to latest version

[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v1.0.2...v1.1.0)

### Stories
* [MODOAIPMH-72](https://issues.folio.org/browse/MODOAIPMH-72) Update inventory interface version

### Bug Fixes
* [MODOAIPMH-73](https://issues.folio.org/browse/MODOAIPMH-73) Default-loaded SRS records won't show up in mod-oai-pmh

## 1.0.2 (released on 02/26/2019)
 * [MODOAIPMH-66](https://issues.folio.org/browse/MODOAIPMH-66) Upgrade to [mod-source-record-storage](https://github.com/folio-org/mod-source-record-storage) 2.0 interface
## 1.0.1 (released on 11/30/2018)
 * The [mod-source-record-storage](https://github.com/folio-org/mod-source-record-storage) is enabled by default in scope of [MODOAIPMH-63](https://issues.folio.org/browse/MODOAIPMH-63). To enable usage of [mod-inventory-storage](https://github.com/folio-org/mod-inventory-storage), the `-Drepository.storage=INVENTORY` VM option should be specified.
## 1.0.0 (released on 11/22/2018)
 * Initial commit (see [MODOAIPMH-2](https://issues.folio.org/browse/MODOAIPMH-2) for more details)
 * The following schemas included in scope of [MODOAIPMH-6](https://issues.folio.org/browse/MODOAIPMH-6):
   + OAI-PMH Schema: [OAI-PMH.xsd](http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#OAIPMHschema) for more dtails)
   + XML Schema for Dublin Core without qualification: [oai_dc.xsd](http://www.openarchives.org/OAI/2.0/oai_dc.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore) for more dtails)
   + MARC 21 XML Schema: [MARC21slim.xsd](http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd) (please refer to [MARC 21 XML Schema](http://www.loc.gov/standards/marcxml/) for more details)
 * RAML defined in scope of [MODOAIPMH-3](https://issues.folio.org/browse/MODOAIPMH-3)
 * Module/Deployment Descriptors added in scope of [MODOAIPMH-4](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-4)
 * The initial implementation of the verb [Identify](https://www.openarchives.org/OAI/openarchivesprotocol.html#Identify) is done in scope of [MODOAIPMH-15](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-15). The system properties are used for now to prepare response:
   + `repository.name` - the name of the repository which is used to construct value for `OAI-PMH/Identify/repositoryName` element.
   + `repository.baseURL` - the URL of the repository (basically the URL of the edge-oai-pmh) to be returned in `OAI-PMH/Identify/baseURL` element.
   + `repository.adminEmails` - the e-mail address of an administrator(s) of the repository to be returned in `OAI-PMH/Identify/adminEmail` element(s). Might contain several emails which should be separated by comma.

   The default values can be overwritten by VM arguments e.g. `-Drepository.name=Specific_FOLIO_OAI-PMH_Repository`
   Please refer to [config.properties](src/main/resources/config/config.properties) to check all the properties used.
   Also there is possibility to specify another configuration file via `-DconfigPath=<path_to_configs>` but the file should be accessible by ClassLoader

 * The initial implementation of the verb [ListSets](https://www.openarchives.org/OAI/openarchivesprotocol.html#ListSets) is done in scope of [MODOAIPMH-14](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-14) 
 * The initial implementation of the verb [ListMetadataFormats](https://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) is done in scope of [MODOAIPMH-16](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-16). There are 2 `metadataPrefix`'s supported: `oai_dc` and `marc21` 
 * The initial implementation of the verb [ListIdentifiers](https://www.openarchives.org/OAI/openarchivesprotocol.html#ListIdentifiers) is done in scope of [MODOAIPMH-20](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-20).
   The [OAI Identifier Format](http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm) is used for identifiers within OAI-PMH. Please refer to [MODOAIPMH-36](https://issues.folio.org/browse/MODOAIPMH-36) for more details
 * The initial implementation of the verb [ListRecords](https://www.openarchives.org/OAI/openarchivesprotocol.html#ListRecords) is done in scope of [MODOAIPMH-12](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-12).
 * The initial implementation of the verb [GetRecord](https://www.openarchives.org/OAI/openarchivesprotocol.html#GetRecord) is done in scope of [MODOAIPMH-17](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-17).
 * The initial implementation of the [Flow Control](http://www.openarchives.org/OAI/openarchivesprotocol.html#FlowControl) is done for ListRecords and ListIdentifiers verbs in scope of [MODOAIPMH-10](https://issues.folio.org/projects/MODOAIPMH/issues/MODOAIPMH-10). The "Encoding State" strategy is implemented. The following system property is used:
   + `repository.maxRecordsPerResponse` - the maximum number of records returned in the response. The default value is 100, but it can be overwritten by VM argument, e.g. `-Drepository.maxRecordsPerResponse=1000` 
 * The compression support is enabled in scope of [MODOAIPMH-53](https://issues.folio.org/browse/MODOAIPMH-53) which is provided by Vert.x (please refer to [HTTP Compression](https://vertx.io/docs/vertx-core/java/#_http_compression) section of the [Vert.x Core Manual](https://vertx.io/docs/vertx-core/java/)). The compression support is activated by default in RMB [v23.1.0](https://github.com/folio-org/raml-module-builder/releases/tag/v23.1.0)
 * The integration with [mod-configuration](https://github.com/folio-org/mod-configuration) has been done in scope of [MODOAIPMH-13](https://issues.folio.org/browse/MODOAIPMH-13). Full support of the configuration per tenant has been done in scope of [MODOAIPMH-62](https://issues.folio.org/browse/MODOAIPMH-62).
 * The initial integration with [mod-source-record-storage](https://github.com/folio-org/mod-source-record-storage) is done in scope of [MODOAIPMH-57](https://issues.folio.org/browse/MODOAIPMH-57). New system property introduced `repository.storage` with 2 allowed values: `INVENTORY` or `SRS`. Default value is `INVENTORY` so [mod-inventory-storage](https://github.com/folio-org/mod-inventory-storage) is used to get MARC records. To enable usage of source-record-storage, the `-Drepository.storage=SRS` VM option should be specified.

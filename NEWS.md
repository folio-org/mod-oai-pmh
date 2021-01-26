## 3.2.8 (Released) 2021-01-26

This release includes the bug fixes and improvements regarding marc21_withholdings metadata prefix processing.

* [MODOAIPMH-282](https://issues.folio.org/browse/MODOAIPMH-282) On 7M records first response to the initial requests with marc21_withholdings metadataPrefix takes more than 20 min
* [MODOAIPMH-284](https://issues.folio.org/browse/MODOAIPMH-284) Return response immediately after the required number of instances will be loaded instead of waiting for completion of all instances loading.

  [Full Changelog](https://github.com/folio-org/mod-data-export/compare/v3.2.7...v3.2.8)

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

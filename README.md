# mod-oai-pmh

Copyright (C) 2019-2023 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.


Portions of this software may use XML schemas Copyright © 2011 [DCMI](http://dublincore.org/), the Dublin Core Metadata Initiative. These are licensed under the [Creative Commons 3.0 Attribution license](http://creativecommons.org/licenses/by/3.0/).

## Introduction

Backend Module implementing the Open Archives Initiative Protocol for Metadata Harvesting ([OAI-PMH Version 2.0](http://www.openarchives.org/OAI/openarchivesprotocol.html)), but providing more RESTful API than described in the specification. 
At the core places the /oai/records endpoint which accepts verb name as main parameter which defines what type of request is and which handler should ve invoked for processing the request.

The following verbs are used: 

Verb | Required parameters | Optional parameters | Exclusive parameters | Response status codes 
------------ | ------------- | ------------- | ------------- | ------------- |
Identify | - | - | - | 200, 400
ListRecords | metadataPrefix | from,until,set| resumptionToken | 200, 400, 404, 422 
ListIdentifiers | metadataPrefix | from,until,set| resumptionToken | 200, 400, 404, 422 
ListSets | - | - | resumptionToken | 200, 400
ListMetadataFormats | - | identifier | - | 200, 400, 404
GetRecord | identifier, metadataPrefix | - | - | 200, 400, 404, 422

The repository supports [oai_dc](https://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore), [marc21](http://www.openarchives.org/OAI/2.0/guidelines-marcxml.htm) and `marc21_withholdings`metadata prefixes. The Latest is used to return holding and item information along with MARC records.
The [OAI Identifier Format](http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm) is used for resource identifiers with the following pattern: `oai:<repositoryBaseUrl>:<tenantId>/<uuid of record>` e.g. ` oai:demo.folio.org:tenant123/fb857902-3ab2-4c34-9772-14ad7acdfe76`.

## Additional information
### Schemas
The following schemas used:
 + OAI-PMH Schema: [OAI-PMH.xsd](http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#OAIPMHschema) for more dtails)
 + XML Schema for Dublin Core without qualification: [oai_dc.xsd](http://www.openarchives.org/OAI/2.0/oai_dc.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore) for more dtails)
 + MARC 21 XML Schema: [MARC21slim.xsd](http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd) (please refer to [MARC 21 XML Schema](http://www.loc.gov/standards/marcxml/) for more details)
### Deployment requirements
OAI-PMH is heavily loaded module and for correct work with big data set(approximately 4-5 millions records) it requires to have as least 400 Mb of java heap and 1Gb for docker container memory.
### Configuration
Configuration properties are intended to be retrieved from [mod-configuration](https://github.com/folio-org/mod-configuration/blob/master/README.md) module. System property values are used as a fallback.
Configurations can be managed from the UI through the mod-configuration via folio settings.
The default configuration system properties split into the logically bounded groups and defined within next 3 json files: [behavior.json](src/main/resources/config/behavior.json), [general.json](src/main/resources/config/general.json), [technical.json](src/main/resources/config/technical.json). 
The configurations by itself are placed within json 'value' field in the "key":"value" way. For stable operation, the application requires the following memory configuration. Java: -XX:MetaspaceSize=384m -XX:MaxMetaspaceSize=512m -Xmx2160m.
Amazon Container: cpu - 2048, memory - 3072, memoryReservation - 2765.

The following configuration properties are used:

Module| Config Code | System Default Value | Description 
------------ | ------------- |-----| --------------------
 |  |
OAI-PMH | `repository.name` | `FOLIO_OAI_Repository` | The name of the repository. The value is used to construct value for `OAI-PMH/Identify/repositoryName` element.
OAI-PMH | `repository.baseURL` | `http://folio.org/oai` | The URL of the repository (basically the URL of the edge-oai-pmh). The value is used  in `OAI-PMH/Identify/baseURL` element.
OAI-PMH | `repository.adminEmails` | `oai-pmh@folio.org` | The e-mail address of an administrator(s) of the repository. Might contain several emails which should be separated by comma. The value is used in `OAI-PMH/Identify/adminEmail` element(s).
OAI-PMH | `repository.timeGranularity` | `YYYY-MM-DDThh:mm:ssZ` | The finest [harvesting granularity](https://www.openarchives.org/OAI/openarchivesprotocol.html#Datestamp) supported by the repository. The legitimate values are `YYYY-MM-DD` and `YYYY-MM-DDThh:mm:ssZ` with meanings as defined in [ISO8601](http://www.w3.org/TR/NOTE-datetime).
OAI-PMH | `repository.deletedRecords` | `persistent` | The manner in which the repository supports the notion of deleted records. Legitimate values are no ; transient ; persistent with meanings defined in the section on deletion.
OAI-PMH | `repository.suppressedRecordsProcessing` | `true`| Boolean value which defines if supressed records should be processed.
OAI-PMH | `repository.maxRecordsPerResponse` | `100` | The maximum number of records returned in the List responses. The main intention is to implement [Flow Control](https://www.openarchives.org/OAI/openarchivesprotocol.html#FlowControl).
OAI-PMH | `jaxb.marshaller.enableValidation` | `false` | Boolean value which defines if the response content should be validated against xsd schemas.
OAI-PMH | `jaxb.marshaller.formattedOutput` | `false` | Boolean value which is used to specify whether or not the marshalled XML data is formatted with linefeeds and indentation.
OAI-PMH | `repository.errorsProcessing` | `500` | Defines in which way OAI-PMH level errors are going to be processed. `200` -  OAI-PMH level error is associated with HTTP status 200. `500` - OAI-PMH level error may be associated with HTTP error status (4xx or 5xx). 
OAI-PMH | `repository.srsHttpRequestRetryAttempts` | `50`| Property is used in marc21_withholdings metadata prefix handler. If SRS returns an incorrect response then the same request will be sent again up to 50 times until the expected response will not be received or all 50 attempts will fail which leads to error response.
OAI-PMH | `repository.srsClientIdleTimeoutSec` | `20` | The idle timeout for requests to SRS.
OAI-PMH | `repository.fetchingChunkSize` | `5000` | The chunk size in batch processing.
OAI-PMH | `repository.recordsSource` | `Source record storage` | Indicates from where instance records are retrieved. Other possible values: Inventory, Source record storage and Inventory.
OAI-PMH | `repository.cleanErrorsInterval` | `30` | The interval for cleaning up old error logs.

### Configuration priority resolving
TenantApi 'POST' implementation is responsible for getting configurations for a module from mod-configuration and adjusting them to system properties when posting module for tenant. Since there 3 places of configurations (mod-configuration, JVM, default form resources), there are ways of resolving configuration inconsistencies when TenantAPI executes. <br/>
First one - if mod-configuration doesn't contain config entry for the particular configuration group then such group will be picked up from the resources with their default values and then will be posted to a mod-configuration. As well if some of them was already defined through JVM property setting up, then such "JVM" value will be used and posted instead of the default one. As well, these values will be used farther within module business logic. <br/>
Second one - if mod-configuration has successfully returned config entry for particular group then such configuration values overrides default and JVM specified as well and these values are set up to system properties and they will be used farther within module business logic. <br/>
So, configurations priority from highest to lowest is the next: mod-configuration (1 priority) -> JVM specified (2 priority) -> default from resources (3 priority) <br/>

Notes: 
* There is an option, as it says at "Configuration priority resolving" paragraph, if mod-configuration doesn't contain a value for configuration, then default or specified via JVM value will be used further as expected, but in the opposite case such value will be used only during InitAPIs execution and will be overridden further with value from mod-configuration after ModTenantAPI execution. Only configPath and repository.storage configurations are an exception here.
* The system default values can be overwritten by VM options e.g. `-Drepository.name=Specific_FOLIO_OAI-PMH_Repository`. 
* Another configuration file can be specified via `-DconfigPath=<path_to_configs>` but the file should be accessible by ClassLoader. 
* For verb `ListRecords` and metadata prefix `marc21_withholdings`, holding and item fields from [mod-inventory-storage](https://github.com/folio-org/mod-inventory-storage)  are returned along with the corresponding records from [mod-source-record-storage](https://github.com/folio-org/mod-source-record-storage)

### About Marc21 with holdings and initial load
Oai-pmh supports marc21_withholdings metadata prefix for ListRecords request. Handling this metadata prefix differs from marc21 and oai_dc. Request processing of marc21_whithholdings involves “initial-load” (IL) process.  Initial load is a process when all instances ids with some metadata are retrieved from inventory via inventory-hierarchy API and put to instances table of the local oia-pmh database. Since there may be a lot of instances, such process can take significant time for processing. For 8 million instances it takes around 6-7 minutes to save instances and respond the first batch of records. Each next batch of records does not take the same time because instances are loaded to DB only once for each harvesting process. After initial-load is completed the first batch of instances ids is processed.<br/>

To differ each set of saved instances between several marc21_withholdings requests/initial-loads the “requestId” param is used. Such request id is generated for each single harvester doing the request and describes the request until harvesting will be ended. Request id is stored among resumptionTokens generated for harvester in scope of single harvesting process. As well request id is stored into next database tables – “request_metadata_lb” and “instances”. First table holds request id as primary key and date column for keeping last updated date of request id.<br/>

At the first time, the last updated date is set during the initial-load and then is updated when each next batch of records is requested via resumptionToken which holds such request id. Instances table as well holds particular requestId as foreign key for associating instance ids with particular harvesting process. By default, each batch of instance ids are removed from database when requesting records via resumptionToken, i.e. when a request with resumption token is sent then required instances ids are processed and then they are cleaned from the database.<br/>

But in a case when harvester lost his resumptionToken then saved instances ids with metadata that have not been retrieved and processed yet will be kept in DB and never will be cleaned. For preventing this case, request id has an expired period which for now equals to one day(24 h.) Therefore, when some request ids with expired last updated date exist then both such request ids and associated with them instances ids start to be considered as expired and will be removed from DB by cleaning job which are run each 2 hours.<br/>

### Harvesting Statistics API

Statistics API contains information on marc21_withholdings harvesting.
The following endpoints can be used to monitor status of the completed harvesting.

Name | Endpoint | Description 
------------ | ------------- | ------------- 
Request Metadata Collection | GET /oai/request-metadata | Returns whole collection of executed harvesting metadata 
Failed to save instances UUIDs | GET /oai/request-metadata/{requestId}/failed-to-save-instances | Returns UUIDs collection of failed to save instances
Failed instances UUIDs | GET /oai/request-metadata/{requestId}/failed-instances | Returns UUIDs collection of failed instances (instances were downloaded but failed to convert in MARC format)
Skipped instances UUIDs| GET /oai/request-metadata/{requestId}/skipped-instances | Returns UUIDs collection of skipped instances (instances were downloaded but there are no corresponding records in SRS)
Suppressed from discovery instances UUIDs| GET /oai/request-metadata/{requestId}/suppressed-from-discovery-instances | Returns UUIDs collection of suppressed from discovery instances

A typical API usage should be performed with the following approach. The user requests a collection of Request Metadata. Finds the necessary request metadata by the harvesting start time. Request Metadata contains the `requestId` 
and counters of the corresponding events. Next, the user can call the necessary endpoints using `requestId` to get a list of UUIDs.

### Environment variables
This module uses S3 storage for files. AWS S3 and Minio Server are supported for files storage.
It is also necessary to specify variable S3_IS_AWS to determine if AWS S3 is used as files storage. By default,
this variable is `false` and means that MinIO server is used as storage.
This value should be `true` if AWS S3 is used.

| Name                         | Default value          | Description                                 |
|:-----------------------------|:-----------------------|:--------------------------------------------|
| S3_URL                       | http://127.0.0.1:9000/ | S3 url                                      |
| S3_REGION                    | -                      | S3 region                                   |
| S3_BUCKET                    | -                      | S3 bucket                                   |
| S3_ACCESS_KEY_ID             | -                      | S3 access key                               |
| S3_SECRET_ACCESS_KEY         | -                      | S3 secret key                               |
| S3_IS_AWS                    | false                  | Specify if AWS S3 is used as files storage  |

### Issue tracker

See project [MODOAIPMH](https://issues.folio.org/browse/MODOAIPMH)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described,
with further FOLIO Developer documentation at
[dev.folio.org](https://dev.folio.org/)

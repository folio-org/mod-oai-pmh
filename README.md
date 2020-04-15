# mod-oai-pmh
Copyright (C) 2019 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.


Portions of this software may use XML schemas Copyright © 2011 [DCMI](http://dublincore.org/), the Dublin Core Metadata Initiative. These are licensed under the [Creative Commons 3.0 Attribution license](http://creativecommons.org/licenses/by/3.0/).

## Introduction

Backend Module implementing the Open Archives Initiative Protocol for Metadata Harvesting ([OAI-PMH Version 2.0](http://www.openarchives.org/OAI/openarchivesprotocol.html)), but providing more RESTful API than described in the specification. Refer to [oai-pmh.raml](ramls/oai-pmh.raml) for the details.

The repository supports [oai_dc](https://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore) and [marc21](http://www.openarchives.org/OAI/2.0/guidelines-marcxml.htm) metadata formats.
The [OAI Identifier Format](http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm) is used for resource identifiers with the following pattern: `oai:<repositoryBaseUrl>:<tenantId>/<uuid of record>` e.g. ` oai:demo.folio.org:tenant123/fb857902-3ab2-4c34-9772-14ad7acdfe76`.

## Additional information
### Schemas
The following schemas used:
 + OAI-PMH Schema: [OAI-PMH.xsd](http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#OAIPMHschema) for more dtails)
 + XML Schema for Dublin Core without qualification: [oai_dc.xsd](http://www.openarchives.org/OAI/2.0/oai_dc.xsd) (please refer to [OAI-PMH specification](http://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore) for more dtails)
 + MARC 21 XML Schema: [MARC21slim.xsd](http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd) (please refer to [MARC 21 XML Schema](http://www.loc.gov/standards/marcxml/) for more details)

### Configuration
Configuration properties are intended to be retrieved from [mod-configuration](https://github.com/folio-org/mod-configuration/blob/master/README.md) module. System property values are used as a fallback.
Configurations can be managed from the UI through the mod-configuration via folio settings.
The default configuration system properties split into the logically bounded groups and defined within next 3 json files: [behavior.json](src/main/resources/config/behavior.json), [general.json](src/main/resources/config/general.json), [technical.json](src/main/resources/config/technical.json). 
The configurations by itself are placed within json 'value' field in the "key":"value" way.

The following configuration properties are used:

Module| Config Code | System Default Value | Description 
------------ | ------------- | ------------- | -------------
 |  |
OAI-PMH | `repository.name` | `FOLIO_OAI_Repository` | The name of the repository. The value is used to construct value for `OAI-PMH/Identify/repositoryName` element.
OAI-PMH | `repository.baseURL` | `http://folio.org/oai` | The URL of the repository (basically the URL of the edge-oai-pmh). The value is used  in `OAI-PMH/Identify/baseURL` element.
OAI-PMH | `repository.adminEmails` | `oai-pmh@folio.org` | The e-mail address of an administrator(s) of the repository. Might contain several emails which should be separated by comma. The value is used in `OAI-PMH/Identify/adminEmail` element(s).
OAI-PMH | `repository.timeGranularity` | `YYYY-MM-DDThh:mm:ssZ` | The finest [harvesting granularity](https://www.openarchives.org/OAI/openarchivesprotocol.html#Datestamp) supported by the repository. The legitimate values are `YYYY-MM-DD` and `YYYY-MM-DDThh:mm:ssZ` with meanings as defined in [ISO8601](http://www.w3.org/TR/NOTE-datetime).
OAI-PMH | `repository.deletedRecords` | `no` | The manner in which the repository supports the notion of deleted records. Legitimate values are no ; transient ; persistent with meanings defined in the section on deletion.
OAI-PMH | `repository.maxRecordsPerResponse` | `100` | The maximum number of records returned in the List responses. The main intention is to implement [Flow Control](https://www.openarchives.org/OAI/openarchivesprotocol.html#FlowControl)
OAI-PMH | `jaxb.marshaller.enableValidation` | `false` | Boolean value which defines if the response content should be validated against xsd schemas.
OAI-PMH | `jaxb.marshaller.formattedOutput` | `false` | Boolean value which is used to specify whether or not the marshalled XML data is formatted with linefeeds and indentation.

### Configuration priority resolving
TenantApi 'POST' implementation is responsible for getting configurations for a module from mod-configuration and adjusting them to system properties when posting module for tenant. Since there 3 places of configurations (mod-configuration, JVM, default form resources), there are ways of resolving configuration inconsistencies when TenantAPI executes. <br/>
First one - if mod-configuration doesn't contain config entry for the particular configuration group then such group will be picked up from the resources with their default values and then will be posted to a mod-configuration. As well if some of them was already defined through JVM property setting up, then such "JVM" value will be used and posted instead of the default one. As well, these values will be used farther within module business logic. <br/>
Second one - if mod-configuration has successfully returned config entry for particular group then such configuration values overrides default and JVM specified as well and these values are set up to system properties and they will be used farther within module business logic. <br/>
So, configurations priority from highest to lowest is the next: mod-configuration (1 priority) -> JVM specified (2 priority) -> default from resources (3 priority) <br/>

Notes: 
* There is an option, as it says at "Configuration priority resolving" paragraph, if mod-configuration doesn't contain a value for configuration, then default or specified via JVM value will be used further as expected, but in the opposite case such value will be used only during InitAPIs execution and will be overridden further with value from mod-configuration after ModTenantAPI execution. Only configPath and repository.storage configurations are an exception here.
* The system default values can be overwritten by VM options e.g. `-Drepository.name=Specific_FOLIO_OAI-PMH_Repository`. 
* Another configuration file can be specified via `-DconfigPath=<path_to_configs>` but the file should be accessible by ClassLoader. 
* There is `repository.storage` system property defining which storage should be used to get records from. This configuration is not tenant specific but system wide therefore cannot be defined in [mod-configuration](https://github.com/folio-org/mod-configuration/) module. There are 2 allowed values:

  | Value | Storage |
  |  ---  |   ---   |  
  | `SRS` | [mod-source-record-storage](https://github.com/folio-org/mod-source-record-storage) |
  | `INVENTORY` | [mod-inventory-storage](https://github.com/folio-org/mod-inventory-storage) |
  
  The default value is `SRS`. To enable usage of the inventory storage, the `-Drepository.storage=INVENTORY` VM option should be specified.

### Issue tracker

See project [MODOAIPMH](https://issues.folio.org/browse/MODOAIPMH)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described,
with further FOLIO Developer documentation at
[dev.folio.org](https://dev.folio.org/)

## 1.0.3 (Released)
Bug fix related to SRS records not showing up, and upgrading instance interface dependency to latest version
[Full Changelog](https://github.com/folio-org/mod-oai-pmh/compare/v1.0.2...v1.0.3)

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

# mod-oai-pmh
Copyright (C) 2018 The Open Library Foundation

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
The default configuration properties are defined in [config.properties](src/main/resources/config/config.properties). The following  properties control response output:
+ `jaxb.marshaller.enableValidation` - boolean value which defines if the response content should be validated against xsd schemas. Enabled by default. Can be disabled by passing `-Djaxb.marshaller.enableValidation=false` JVM argument.
+ `jaxb.marshaller.formattedOutput` - boolean value which is used to specify whether or not the marshalled XML data is formatted with linefeeds and indentation. Disabled by default. Can be enabled by passing `-Djaxb.marshaller.formattedOutput=true` JVM argument.

Note: another configuration file can be specified via `-DconfigPath=<path_to_configs>` but the file should be accessible by ClassLoader

### Issue tracker

See project [MODOAIPMH](https://issues.folio.org/browse/MODOAIPMH)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described,
with further FOLIO Developer documentation at
[dev.folio.org](https://dev.folio.org/)

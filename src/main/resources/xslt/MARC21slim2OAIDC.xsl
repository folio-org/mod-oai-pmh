<xsl:stylesheet version="1.0" exclude-result-prefixes="marc" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
    <xsl:import href="xslt/MARC21slimUtils.xsl"/>
    <xsl:output method="xml" indent="yes"/><!--

	Fixed 530 Removed type="original" from dc:relation 2010-11-19 tmee
	Fixed 500 fields. 2006-12-11 ntra
	Added ISBN and deleted attributes 6/04 jer

-->
    <xsl:template match="/">
        <xsl:if test="marc:collection">
            <oai_dc:dcCollection>
                <xsl:for-each select="marc:collection">
                    <xsl:for-each select="marc:record">
                        <oai_dc:dc>
                            <xsl:apply-templates select="."/>
                        </oai_dc:dc>
                    </xsl:for-each>
                </xsl:for-each>
            </oai_dc:dcCollection>
        </xsl:if>
        <xsl:if test="marc:record">
            <oai_dc:dc
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
                <xsl:apply-templates/>
            </oai_dc:dc>
        </xsl:if>
    </xsl:template>
    <xsl:template match="marc:record">
        <xsl:variable name="leader" select="marc:leader"/>
        <xsl:variable name="leader6" select="substring($leader,7,1)"/>
        <xsl:variable name="leader7" select="substring($leader,8,1)"/>
        <xsl:variable name="controlField008" select="marc:controlfield[@tag=008]"/>
        <xsl:for-each select="marc:datafield[@tag=245]">
            <dc:title>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abfghk</xsl:with-param>
                </xsl:call-template>
            </dc:title>
        </xsl:for-each>
        <xsl:for-each
                select="marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]">
            <dc:creator>
                <xsl:value-of select="."/>
            </dc:creator>
        </xsl:for-each>
        <dc:type>
          <xsl:value-of select="marc:datafield[@tag=336]/marc:subfield[@code='a']"/>
        </dc:type>
        <xsl:for-each select="marc:datafield[@tag=655]">
            <dc:type>
                <xsl:value-of select="."/>
            </dc:type>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=260]">
            <dc:publisher>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">ab</xsl:with-param>
                </xsl:call-template>
            </dc:publisher>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=260]/marc:subfield[@code='c']">
            <dc:date>
                <xsl:value-of select="."/>
            </dc:date>
        </xsl:for-each>
        <dc:language>
            <xsl:value-of select="substring($controlField008,36,3)"/>
        </dc:language>
        <xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='q']">
            <dc:format>
                <xsl:value-of select="."/>
            </dc:format>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=520]">
            <dc:description>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:description>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=521]">
            <dc:description>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:description>
        </xsl:for-each>
        <xsl:for-each
                select="marc:datafield[500&lt;= @tag and @tag&lt;= 599 ][not(@tag=506 or @tag=530 or @tag=540 or @tag=546)]">
            <dc:description>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:description>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=600]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=610]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=611]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=630]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=650]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=653]">
            <dc:subject>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdq</xsl:with-param>
                </xsl:call-template>
            </dc:subject>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=752]">
            <dc:coverage>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcd</xsl:with-param>
                </xsl:call-template>
            </dc:coverage>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=530]">
            <dc:relation>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">abcdu</xsl:with-param>
                </xsl:call-template>
            </dc:relation>
        </xsl:for-each>
        <xsl:for-each
                select="marc:datafield[@tag=760]|marc:datafield[@tag=762]|marc:datafield[@tag=765]|marc:datafield[@tag=767]|marc:datafield[@tag=770]|marc:datafield[@tag=772]|marc:datafield[@tag=773]|marc:datafield[@tag=774]|marc:datafield[@tag=775]|marc:datafield[@tag=776]|marc:datafield[@tag=777]|marc:datafield[@tag=780]|marc:datafield[@tag=785]|marc:datafield[@tag=786]|marc:datafield[@tag=787]">
            <dc:relation>
                <xsl:call-template name="subfieldSelect">
                    <xsl:with-param name="codes">ot</xsl:with-param>
                </xsl:call-template>
            </dc:relation>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=856]">
            <dc:identifier>
                <xsl:value-of select="marc:subfield[@code='u']"/>
            </dc:identifier>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=020]">
            <dc:identifier>
                <xsl:text>URN:ISBN:</xsl:text>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:identifier>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=506]">
            <dc:rights>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:rights>
        </xsl:for-each>
        <xsl:for-each select="marc:datafield[@tag=540]">
            <dc:rights>
                <xsl:value-of select="marc:subfield[@code='a']"/>
            </dc:rights>
        </xsl:for-each>
      <xsl:for-each select="marc:datafield[@tag=999 and @ind1='f' and @ind2='f']">
          <xsl:variable name="suppress" select="marc:subfield[@code='t']" />
            <xsl:if test="string-length($suppress)>0">
                <dc:rights>
                  <xsl:choose>
                    <xsl:when test="$suppress = 0">discovery not suppressed</xsl:when>
                    <xsl:when test="$suppress = 1">discovery suppressed</xsl:when>
                  </xsl:choose>
                </dc:rights>
            </xsl:if>
      </xsl:for-each><!--</oai_dc:dc>-->
    </xsl:template>
</xsl:stylesheet><!--
 Stylus Studio meta-information - (c) 2004-2005. Progress Software Corporation. All rights reserved.
<metaInformation>
<scenarios ><scenario default="yes" name="Scenario1" userelativepaths="yes" externalpreview="no" url="..\..\..\..\..\..\..\..\..\..\javadev4\testsets\diacriticu8.xml" htmlbaseurl="" outputurl="" processortype="internal" useresolver="yes" profilemode="0" profiledepth="" profilelength="" urlprofilexml="" commandline="" additionalpath="" additionalclasspath="" postprocessortype="none" postprocesscommandline="" postprocessadditionalpath="" postprocessgeneratedext="" validateoutput="no" validator="internal" customvalidator=""/></scenarios><MapperMetaTag><MapperInfo srcSchemaPathIsRelative="yes" srcSchemaInterpretAsXML="no" destSchemaPath="" destSchemaRoot="" destSchemaPathIsRelative="yes" destSchemaInterpretAsXML="no"/><MapperBlockPosition></MapperBlockPosition><TemplateContext></TemplateContext><MapperFilter side="source"></MapperFilter></MapperMetaTag>
</metaInformation>
-->

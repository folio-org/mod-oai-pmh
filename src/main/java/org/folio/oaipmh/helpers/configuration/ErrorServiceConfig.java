package org.folio.oaipmh.helpers.configuration;

import lombok.extern.log4j.Log4j2;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.folio.s3.exception.S3ClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Log4j2
public class ErrorServiceConfig {

  @Value("${minio.endpoint}")
  private String endpoint;

  @Value("${minio.region}")
  private String region;

  @Value("${minio.bucket}")
  private String bucket;

  @Value("${minio.accessKey}")
  private String accessKey;

  @Value("${minio.secretKey}")
  private String secretKey;

  @Value("#{ T(Boolean).parseBoolean('${minio.awsSdk}')}")
  private boolean awsSdk;

  @Bean
  public FolioS3Client folioS3Client() {
    log.info("Folio S3 client for error storage: endpoint {}, region {}, bucket {}, accessKey {}, secretKey {}, awsSdk {}",
      endpoint, region, bucket, "<secret>", secretKey, awsSdk);
    var client = S3ClientFactory.getS3Client(S3ClientProperties.builder()
      .endpoint(endpoint)
      .secretKey(secretKey)
      .accessKey(accessKey)
      .bucket(bucket)
      .awsSdk(awsSdk)
      .region(region)
      .build());
    try {
      client.createBucketIfNotExists();
    } catch (S3ClientException exc) {
      log.error("Error creating bucket {} during S3 client initialization", bucket);
    }
    return client;
  }
}

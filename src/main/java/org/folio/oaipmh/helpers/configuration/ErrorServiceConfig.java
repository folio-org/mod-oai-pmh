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

  @Value("${S3_URL}")
  private String endpoint;

  @Value("${S3_REGION}")
  private String region;

  @Value("${S3_BUCKET}")
  private String bucket;

  @Value("${S3_ACCESS_KEY_ID}")
  private String accessKey;

  @Value("${S3_SECRET_ACCESS_KEY}")
  private String secretKey;

  @Value("${S3_IS_AWS}")
  private Boolean awsSdk;

  @Bean
  public FolioS3Client folioS3Client() {
    log.info("Folio S3 client for error storage: endpoint {}, region {}, bucket {}, accessKey {}, secretKey {}, awsSdk {}",
      endpoint, region, bucket, accessKey, secretKey, awsSdk);
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

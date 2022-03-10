buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'no'

  doApiLint = true
  doApiDoc = true
  apiTypes = 'RAML'
  apiDirectories = 'ramls'

  buildNode = 'jenkins-agent-java11'
  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      // health check in ModTenantAPIIT
    }
  }
}

buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'no'

  buildNode = 'jenkins-agent-java21'
  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      // health check in ModTenantAPIIT
    }
  }
}

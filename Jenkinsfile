buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'no'

  buildNode = 'jenkins-agent-java17'
  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      // health check in ModTenantAPIIT
    }
  }
}

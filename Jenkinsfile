buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'no'

  buildNode = 'jenkins-agent-java11'
  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      // health check in ModTenantAPIIT
    }
  }
}

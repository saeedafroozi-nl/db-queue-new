library identifier: 'jenkins-backend-platform-jobs@master',
        retriever: modernSCM([
                $class       : 'GitSCMSource',
                credentialsId: "svcBbSynchronizer",
                remote       : 'ssh://git@bitbucket.yooteam.ru/backend-tools/jenkins-backend-platform-jobs.git',
                traits       : [[$class: 'IgnoreOnPushNotificationTrait']]
        ])

artifactPipeline(
        buildDockerImage: defaultBuildDockerImage.IMAGE_JAVA_17
)
resolvers ++= Seq(
    Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns),
    Resolver.url("typesafe-ivy", url("http://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns),
    "OSS Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "OSS Sonatype Snaps" at "https://oss.sonatype.org/content/repositories/snapshots/")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.1")

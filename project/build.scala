/*
 * Copyright (c) 2013 Novus Partners, Inc. (http://www.novus.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object NovusjdbcBuild extends sbt.Build {

  artifact in (Compile, assembly) ~= { _.copy(`classifier` = Some("assembly")) }

  lazy val root = Project(
    id = "novus-jdbc",
    base = file("."),
    settings = baseSettings
  ).aggregate(novusJdbc, novusJdbcBonecp, novusJdbcDBCP, novusJdbcC3P0, novusJdbcTomcat, novusJdbcLogging)
  
  lazy val novusJdbc = Project(
    id = "novus-jdbc-core",
    base = file("novus-jdbc-core"),
    settings = baseSettings ++
      Seq(libraryDependencies <++= scalaVersion (v => Seq(
        "net.sourceforge.jtds" % "jtds" % "1.2.6",
        "org.slf4j" % "slf4j-api" % "1.7.2",
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2" % "compile",
        "org.hsqldb" % "hsqldb" % "2.2.9" % "test"
    ) ++ Shared.specsDep(v))))

  lazy val novusJdbcBonecp = Project(
    id = "novus-jdbc-bonecp",
    base = file("novus-jdbc-bonecp"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion(v => Seq(
      "com.jolbox" % "bonecp" % "0.7.1.RELEASE"
    ) ++ Shared.specsDep(v)))
  ).dependsOn(novusJdbc)

  lazy val novusJdbcDBCP = Project(
    id = "novus-jdbc-dbcp",
    base = file("novus-jdbc-dbcp"),
    settings = baseSettings ++Seq(libraryDependencies <++= scalaVersion (v => Seq(
      "commons-dbcp" % "commons-dbcp" % "1.4"
    ))))
    .dependsOn(novusJdbc)

  lazy val novusJdbcC3P0 = Project(
    id = "novus-jdbc-c3p0",
    base = file("novus-jdbc-c3p0"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion (v => Seq(
      "c3p0" % "c3p0" % "0.9.1.2" //technically 0.9.2 is latest but need to download it
    ))))
    .dependsOn(novusJdbc)

  lazy val novusJdbcTomcat = Project(
    id = "novus-jdbc-tomcat",
    base = file("novus-jdbc-tomcat"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion (v => Seq(
      "org.apache.tomcat" % "tomcat-jdbc" % "7.0.37"
    ))))
    .dependsOn(novusJdbc)

  lazy val novusJdbcLogging = Project(
    id = "novus-jdbc-logging",
    base = file("novus-jdbc-logging"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion (v => Seq(
      "ch.qos.logback" % "logback-classic" % "1.0.7"
    ) ++ Shared.specsDep(v))))

  lazy val baseSettings = Project.defaultSettings ++ Seq(
    organization := "com.novus",
    version := "0.9.7-SNAPSHOT",
    scalaVersion := "2.10.3",
    initialCommands := "import com.novus.jdbc._",
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature", "-language:postfixOps"),
    resolvers ++= Seq(
      "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots",
      "Sonatype OSS snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "Sonatype OSS releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),
    pomIncludeRepository := { _ => false },
    pomExtra := (
      <url>https://github.com/novus/novus-jdbc</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:novus/novus-jdbc.git</url>
        <connection>scm:git:git@github.com:novus/novus-jdbc.git</connection>
      </scm>
      <developers>
        <developer>
          <id>wheaties</id>
          <name>Owein Reese</name>
          <url>staticallytyped.wordpress.com</url>
          <organizationUrl>https://www.novus.com/</organizationUrl>
        </developer>
      </developers>
    ),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo <<= version { (version: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (version.trim.endsWith("SNAPSHOT")) Some("Sonatype OSS deployment snapshots" at nexus + "content/repositories/snapshots")
      else Some("Sonatype OSS deployment releases" at nexus + "service/local/staging/deploy/maven2")
    }) ++ assemblySettings
}

object Shared {

  val mockito = "org.mockito" % "mockito-all" % "1.9.0"

  /** Resolve specs version for the current scala version (thanks @n8han). */
  def specsDep(sv: String, cfg: String = "test") =
    (sv.split("[.-]").toList match {
      case "2" :: "9" :: "0" :: _ => "org.specs2" % "specs2_2.9.1" % "1.7.1" :: mockito :: Nil
      case "2" :: "9" :: _ :: _ => "org.specs2" % "specs2_2.9.1" % "1.8.2" :: mockito :: Nil
      case "2" :: "10" :: _ => "org.specs2" % "specs2_2.10" % "1.14" :: mockito :: Nil
      case _ => sys.error("Specs not supported for scala version %s" format sv)
    }) map (_ % cfg)
  
}

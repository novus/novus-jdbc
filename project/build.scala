import sbt._
import Keys._

object NovusjdbcBuild extends sbt.Build {

  /** Default project uses Bonecp as the underlying pool. */
  lazy val root = Project(
    id = "novus-jdbc",
    base = file(".")
  ).aggregate(novusJdbc, novusJdbcBonecp, novusJdbcLogging)
  
  lazy val novusJdbc = Project(
    id = "novus-jdbc-core",
    base = file("novus-jdbc-core"),
    settings = baseSettings ++
      Seq(libraryDependencies <++= scalaVersion (v => Seq(
        "net.sourceforge.jtds" % "jtds" % "1.2.6",
        "org.slf4j" % "slf4j-api" % "1.7.2",
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2" % "compile"
    ) ++ Shared.specsDep(v))))

  lazy val novusJdbcBonecp = Project(
    id = "novus-jdbc-bonecp",
    base = file("novus-jdbc-bonecp"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion (v => Seq(
        "com.jolbox" % "bonecp" % "0.7.1.RELEASE",
        "org.slf4j" % "slf4j-api" % "1.7.2"
    ) ++ Shared.specsDep(v))))
    .dependsOn(novusJdbc)

  lazy val novusJdbcLogging = Project(
    id = "novus-jdbc-logging",
    base = file("novus-jdbc-logging"),
    settings = baseSettings ++ Seq(libraryDependencies <++= scalaVersion (v => Seq(
        "org.slf4j" % "slf4j-api" % "1.7.2",
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2" % "compile"
    ) ++ Shared.specsDep(v))))

  lazy val baseSettings = Project.defaultSettings ++ Seq(
    organization := "com.novus",
    version := "0.7.1-SNAPSHOT",
    scalaVersion := "2.9.2",
    crossScalaVersions := Seq("2.8.1", "2.9.0", "2.9.0-1", "2.9.1"),
    initialCommands := "import com.novus.jdbc._",
    resolvers ++= Seq(
      "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots",
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),
    credentials += Credentials(Path.userHome / ".ivy2" / ".novus_nexus"),
    publishTo <<= (version) { version: String =>
      val sfx = if(version.trim.endsWith("SNAPSHOT")) "snapshots" else "releases"
      val nexus = "https://nexus.novus.com:65443/nexus/content/repositories/"
      Some("Novus " + sfx at nexus + sfx + "/")
    })
}

object Shared {

  val mockito = "org.mockito" % "mockito-all" % "1.9.0"

  /** Resolve specs version for the current scala version (thanks @n8han). */
  def specsDep(sv: String, cfg: String = "test") =
    (sv.split("[.-]").toList match {
      case "2" :: "8" :: "1" :: Nil =>
        "org.specs2" %% "specs2" % "1.5" ::
        "org.specs2" %% "specs2-scalaz-core" % "5.1-SNAPSHOT" ::
        mockito :: Nil
      case "2" :: "9" :: "0" :: _ => "org.specs2" % "specs2_2.9.1" % "1.7.1" :: mockito :: Nil
      case "2" :: "9" :: _ :: _ => "org.specs2" % "specs2_2.9.1" % "1.8.2" :: mockito :: Nil
      //case "2" :: "9" :: "2" :: _ => "org.specs2" % "specs2_2.9.2" % "1.13" :: mockito :: Nil
      case _ => sys.error("Specs not supported for scala version %s" format sv)
    }) map (_ % cfg)
  
}

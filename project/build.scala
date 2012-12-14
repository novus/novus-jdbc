import sbt._
import Keys._

object NovusjdbcBuild extends sbt.Build {
  
  lazy val novusjdbc =
    Project(id = "novus-jdbc",
            base = file("."),
            settings = Project.defaultSettings ++ Seq(
              organization := "com.novus",
              version := "0.2.0-SNAPSHOT",
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
                val sfx =
                  if(version.trim.endsWith("SNAPSHOT"))
                    "snapshots"
                  else
                    "releases"
                val nexus = "https://nexus.novus.com:65443/nexus/content/repositories/"
                Some("Novus " + sfx at nexus + sfx + "/")
              },
              libraryDependencies <++= scalaVersion (v => Seq(
                "com.jolbox" % "bonecp" % "0.7.1.RELEASE" withSources(),
                "net.sourceforge.jtds" % "jtds" % "1.2.4" withSources(),
                "ch.qos.logback" % "logback-classic" % "0.9.26" withSources(),
                "joda-time" % "joda-time" % "2.1" withSources(),
                "org.joda" % "joda-convert" % "1.2" % "compile" withSources()
            ) ++ Shared.specsDep(v))))
}

object Shared {
  
  /** Resolve specs version for the current scala version (thanks @n8han). */
  def specsDep(sv: String, cfg: String = "test") =
    (sv.split("[.-]").toList match {
      case "2" :: "8" :: "1" :: Nil =>
        "org.specs2" %% "specs2" % "1.5" ::
        "org.specs2" %% "specs2-scalaz-core" % "5.1-SNAPSHOT" ::
        Nil
      case "2" :: "9" :: "0" :: _ => "org.specs2" % "specs2_2.9.1" % "1.7.1" :: Nil
      case "2" :: "9" :: _ :: _ => "org.specs2" % "specs2_2.9.1" % "1.8.2" :: Nil
      case _ => sys.error("Specs not supported for scala version %s" format sv)
    }) map (_ % cfg)
  
}

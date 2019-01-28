import sbt._

lazy val commonSettings = Seq(
  // sbt-git provides the isSnapshot.value variable, which will return true if there
  // are no tags associated with the HEAD commit, or if there are uncommitted
  // changes. However, sbt-git only appends the -SNAPSHOT suffix if there are
  // uncommitted changes in the workspace. 
  //
  // https://github.com/sbt/sbt-git/blob/f8caf9365be380cf101e9605af159b5e7f842d0c/src/main/scala/com/typesafe/sbt/SbtGit.scala#L173
  version := {
    // Avoid Cyclic reference involving error
    /*if (git.gitCurrentTags.value.isEmpty || git.gitUncommittedChanges.value)
      git.gitDescribedVersion.value.get + "-SNAPSHOT"
    else
      git.gitDescribedVersion.value.get*/
    
    "0.17.12"
  },
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
  scalacOptions ++= Seq(
    "-deprecation", "-unchecked", "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-Ypartial-unification" // Required by Cats
  ),
  addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.9" cross CrossVersion.binary),
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "boundless repo" at "https://repo.boundlessgeo.com/main/"
  ),
  Test / fork := true,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF"),
  javaOptions ++= Seq("-Djava.library.path=/usr/local/lib")
) ++ publishSettings

lazy val publishSettings = Seq(
  // Project metadata
  // https://www.scala-sbt.org/0.13/docs/Howto-Project-Metadata.html
  description := "GeoTrellis GDAL Bindings",
  organization := "com.azavea.geotrellis", // groupId
  homepage := Some(url("https://github.com/geotrellis/geotrellis-gdal")),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  // https://github.com/sbt/sbt-header
  headerLicense := Some(HeaderLicense.ALv2("2019", "Azavea")),
  // Bintray metadata
  // https://github.com/sbt/sbt-bintray
  bintrayOrganization := Some("azavea"),
  bintrayRepository := "geotrellis",
  bintrayVcsUrl := Some("https://github.com/geotrellis/geotrellis-gdal.git"),
  bintrayReleaseOnPublish := false,
  // Generate and publish POM file instead of Ivy file
  // https://www.scala-sbt.org/1.x/docs/Publishing.html
  publishMavenStyle := true,
  // https://www.scala-sbt.org/1.x/docs/Artifacts.html
  publishArtifact in Test := false,
  // Remove all additional repository other than Maven Central from POM
  // https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html
  pomIncludeRepository := { _ => false },
  // https://www.scala-sbt.org/1.x/docs/Publishing.html
  pomExtra := (
    <developers>
      <developer>
        <id>echeipesh</id>
        <name>Eugene Cheipesh</name>
        <url>http://github.com/echeipesh/</url>
      </developer>
      <developer>
        <id>lossyrob</id>
        <name>Rob Emanuele</name>
        <url>http://github.com/lossyrob/</url>
      </developer>
      <developer>
        <id>hjaekel</id>
        <name>Holger Jaekel</name>
        <url>http://github.com/hjaekel/</url>
      </developer>
      <developer>
        <id>schBen</id>
        <name>Benjamin Schmeichel</name>
        <url>http://github.com/schben/</url>
      </developer>
    </developers>
  ),
  // sbt-bintray doesn't have any support for publishing to oss.jfrog.org (OJO)
  publishTo := {
    val bintrayPublishTo = publishTo.value
    if (isSnapshot.value) 
      Some("OJO" at "https://oss.jfrog.org/artifactory/oss-snapshot-local")
    else 
      bintrayPublishTo
  },
  // Credentials for publishing SNAPSHOT artifacts to OJO
  // http://szimano.org/automatic-deployments-to-jfrog-oss-and-bintrayjcentermaven-central-via-travis-ci-from-sbt/
  credentials ++= (for {
    username <- Option(System.getenv().get("BINTRAY_USER"))
    password <- Option(System.getenv().get("BINTRAY_PASS"))
  } yield Credentials("Artifactory Realm", "oss.jfrog.org", username, password)).toSeq
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .aggregate(gdal, `gdal-etl`)

lazy val `gdal-etl` = project
  .settings(commonSettings: _*)
  .settings(name := "geotrellis-gdal-spark-etl")
  .settings(libraryDependencies ++= Seq(
    Dependencies.geotrellisSpark % Provided,
    Dependencies.geotrellisSparkEtl % Provided,
    Dependencies.sparkCore % Provided,
    Dependencies.hadoopClient % Provided
  ))
  .dependsOn(`gdal`, `gdal-spark`)

lazy val `gdal-spark` = project
  .settings(commonSettings: _*)
  .settings(name := "geotrellis-gdal-spark")
  .settings(libraryDependencies ++= Seq(
    Dependencies.geotrellisSpark % Provided,
    Dependencies.geotrellisSparkTestkit % Test,
    Dependencies.sparkCore % Provided,
    Dependencies.sparkSQL % Provided,
    Dependencies.hadoopClient % Provided,
    Dependencies.scalaTest % Test
  ))
  .dependsOn(`gdal`)

lazy val `gdal` = project
  .settings(commonSettings: _*)
  .settings(name := "geotrellis-gdal")
  .settings(libraryDependencies ++= Seq(
    Dependencies.geotrellisRaster % Provided,
    Dependencies.geotrellisRasterTestkit % Test,
    Dependencies.gdal,
    Dependencies.scopt,
    Dependencies.catsEffect % Test
  ))

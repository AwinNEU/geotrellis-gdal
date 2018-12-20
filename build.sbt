import sbt._

lazy val commonSettings = Seq(
  version := Version.geotrellisGdal,
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
  description := "GeoTrellis GDAL Bindings",
  organization := "com.azavea.geotrellis",
  name := "geotrellis-gdal",
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  headerLicense := Some(HeaderLicense.ALv2("2017", "Azavea")),
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
  bintrayOrganization := Some("azavea"),
  bintrayRepository := "geotrellis",
  bintrayVcsUrl := Some("https://github.com/geotrellis/geotrellis-gdal.git"),
  homepage := Some(url("https://github.com/geotrellis/geotrellis-gdal")),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary),
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  pomExtra := (
    <scm>
      <url>git@github.com:geotrellis/geotrellis-gdal.git</url>
      <connection>scm:git:git@github.com:geotrellis/geotrellis-gdal.git</connection>
    </scm>
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
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
    "boundless repo" at "https://repo.boundlessgeo.com/main/"
  ),
  fork in Test := true,
  parallelExecution in Test := false,
  javaOptions in Test += s"-Djava.library.path=${Environment.javaLibraryPath}"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .aggregate(gdal, `gdal-etl`)

lazy val `gdal-etl` = project
  .settings(commonSettings: _*)
  .settings(name := "geotrellis-spark-gdal-etl")
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
    Dependencies.scopt
  ))

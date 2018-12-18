/*
 * Copyright (c) 2017 Azavea.
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

object Dependencies {
  val gdal = "org.gdal" % "gdal" % Version.gdal

  val geotrellisRaster        = "org.locationtech.geotrellis" %% "geotrellis-raster"         % Version.geotrellis
  val geotrellisRasterTestkit = "org.locationtech.geotrellis" %% "geotrellis-raster-testkit" % Version.geotrellis
  val geotrellisSpark         = "org.locationtech.geotrellis" %% "geotrellis-spark"          % Version.geotrellis
  val geotrellisSparkEtl      = "org.locationtech.geotrellis" %% "geotrellis-spark-etl"      % Version.geotrellis
  val geotrellisSparkTestkit  = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit"  % Version.geotrellis

  val sparkCore    = "org.apache.spark"  %% "spark-core"    % Version.spark
  val sparkSQL     = "org.apache.spark"  %% "spark-sql"     % Version.spark
  val hadoopClient = "org.apache.hadoop" %  "hadoop-client" % Version.hadoop

  val scalaTest  = "org.scalatest" %% "scalatest" % "3.0.5"
  val scopt      = "com.github.scopt"            %% "scopt"       % "3.7.0"
}

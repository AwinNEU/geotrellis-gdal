/*
 * Copyright (c) 2014 Azavea.
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
import scala.util.Properties

object Version {
  val scala           = "2.11.12"
  val crossScala      = Seq(scala, "2.12.8")
  val geotrellis      = "2.2.0"
  val gdal            = "2.3.3"
  lazy val hadoop     = Properties.envOrElse("SPARK_HADOOP_VERSION", "2.8.5")
  lazy val spark      = Properties.envOrElse("SPARK_VERSION", "2.4.0")
}

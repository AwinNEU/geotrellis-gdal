/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.gdal

import java.io.File

case class GDALInfoOptions(
  // -mm
  computeMinMax: Boolean = false,
  // -stats
  showStats: Boolean = false,
  // -approx_stats
  showApproxStats: Boolean = true,
  // -hist
  showHistogram: Boolean = false,
  // -nogcp
  showGcps: Boolean = true,
  // -nomd
  showMetadata: Boolean = true,
  // -noct
  showColorTable: Boolean = true,
  // -norat
  showRAT:Boolean = true,
  // -checksum
  computeChecksum: Boolean = false,
  // -mdd
  mdds: Seq[String] = Seq(),
  // Positional Argument
  file: File = null
)

object GDALInfoOptions {
  val parser = new scopt.OptionParser[GDALInfoOptions]("gdalinfo") {
    head("gdalinfo")

    opt[Unit]("mm") action { (_, c) =>
      c.copy(computeMinMax = true)
    } text("Force computation of the actual min/max values for each band in the dataset.")

    opt[Unit]("stats") action { (_, c) =>
      c.copy(showStats = true)
      c.copy(showApproxStats = false)
    } text("Read and display image statistics. Force computation if no statistics are stored in an image.")

    opt[Unit]("approx_stats") action { (_, c) =>
      c.copy(showApproxStats = false)
      c.copy(showStats = true)
    } text("Read and display image statistics. Force computation if no statistics are stored in an image. However, they may be computed based on overviews or a subset of all tiles. Useful if you are in a hurry and don't want precise stats.")

    opt[Unit]("hist") action { (_, c) =>
      c.copy(showHistogram = true)
    } text("Report histogram information for all bands.")

    opt[Unit]("nogcp") action { (_, c) =>
      c.copy(showGcps = false)
    } text("Suppress ground control points list printing. It may be useful for datasets with huge amount of GCPs, such as L1B AVHRR or HDF4 MODIS which contain thousands of them.")

    opt[Unit]("noct") action { (_, c) =>
      c.copy(showColorTable = false)
    } text("Suppress printing of color table.")

    opt[Unit]("nomd") action { (_, c) =>
      c.copy(showMetadata = false)
    } text("Suppress metadata printing. Some datasets may contain a lot of metadata strings.")

    opt[Unit]("norat") action { (_, c) =>
      c.copy(showColorTable = false)
    } text("Suppress printing of raster attribute table.")

    opt[Unit]("checksum") action { (_, c) =>
      c.copy(computeChecksum = true)
    } text("Force computation of the checksum for each band in the dataset.")

    opt[String]("mdd") unbounded() optional() action { (mdd, c) =>
      c.copy(mdds = c.mdds :+ mdd)
    } text("Report metadata for the specified domain. Starting with GDAL 2.0, 'all' can be used to report metadata in all domains.")

    arg[File]("<file>") required() action { (f: File, c) =>
      c.copy(file = f)
    } text("Path to a GDAL supported raster dataset.")
  }

  def parse(args: Array[String]): Option[GDALInfoOptions] =
    parser.parse(args, GDALInfoOptions())
      .flatMap { options =>
        // Validate here where we don't want to print usage
        // on failure.
        if(!options.file.exists) {
          System.err.println(s"ERROR: File ${options.file} does not exist.")
          None
        } else {
          Some(options)
        }
      }
}


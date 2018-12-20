/*
 * Copyright 2017 Azavea
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

import java.awt.Color

import org.gdal.osr.{CoordinateTransformation, SpatialReference}

object GDALInfo {
  def main(args:Array[String]): Unit =
    GDALInfoOptions.parse(args) match {
      case Some(options) =>
        apply(options)
      case None =>
        // Argument errored, should have printed usage.
    }

  def apply(options: GDALInfoOptions): Unit = {
    sgdal.AllRegister

    val raster = GDAL.open(options.file.getAbsolutePath)

    val driver = raster.getDriver

    println(s"Driver: ${driver.getShortName}/${driver.getLongName}")

    val fileList = raster.getFileList
    if(fileList.isEmpty) {
      println("Files: none associated")
    } else {
      println("Files:")
      for(f <- fileList) { println(s"       $f") }
    }

    println(s"Size is ${raster.getRasterXSize}, ${raster.getRasterYSize}")

    raster.getProjectionRef match {
      case Some(projection) =>
        val srs = new SpatialReference(projection)
        if(srs != null && projection.length != 0) {
          val arr = Array.ofDim[String](1)
          srs.ExportToPrettyWkt(arr)
          println("Coordinate System is:")
          println(arr(0))
        } else {
          println(s"Coordinate Sytem is ${projection}")
        }
        if(srs != null) { srs.delete() }
      case None =>
    }

    val geoTransform = raster.getGeoTransform

    if (geoTransform(2) == 0.0 && geoTransform(4) == 0.0) {
      println(s"Origin = (${geoTransform(0)},${geoTransform(3)})")
      println(s"Pixel Size = (${geoTransform(1)},${geoTransform(5)})")
    } else {
      println("GeoTransform =")
      println(s"  ${geoTransform(0)}, ${geoTransform(1)}, ${geoTransform(2)}")
      println(s"  ${geoTransform(3)}, ${geoTransform(4)}, ${geoTransform(5)}")
    }

    if(options.showGcps && raster.getGCPCount > 0) {
      for((gcp, i) <- raster.getGCPs.zipWithIndex) {
        println(s"GCP[$i]: Id=${gcp.getId}, Info=${gcp.getInfo}")
        // col, row, x, y, x
        println(s"    (${gcp.getGCPPixel},${gcp.getGCPLine}) (${gcp.getGCPX},${gcp.getGCPY},${gcp.getGCPZ})")
      }
    }

    if(options.showMetadata) {
      def printMetadata(header: String, id: String = "") = {
        val md = raster.getMetadata_List(id)
        if(md.nonEmpty) {
          println(header)
          for(key <- md) {
            println(s"  $key")
          }
        }
      }

      val metadataPairs = List(
        ("Image Structure Metadata:", "IMAGE_STRUCTURE"),
        ("Subdatasets:", "SUBDATASETS"),
        ("Geolocation:", "GEOLOCATION"),
        ("RPC Metadata:", "RPC")
      )

      printMetadata("Metadata:")

      for(domain <- options.mdds) {
        printMetadata("Metadata ($domain):", domain)
      }

      for((header, id) <- metadataPairs) {
        printMetadata(header, id)
      }
    }

    println("Corner Coordinates:")
    reportCorner(raster, "Upper Left ", 0.0, 0.0)
    reportCorner(raster, "Lower Left ", 0.0, raster.getRasterYSize)
    reportCorner(raster, "Upper Right", raster.getRasterXSize, 0.0)
    reportCorner(raster, "Lower Right", raster.getRasterXSize, raster.getRasterYSize)
    reportCorner(raster, "Center     ", raster.getRasterXSize / 2.0, raster.getRasterYSize / 2.0)

    for ((band, i) <- raster.getRasterBands().zipWithIndex) {
      print(s"Band ${i+1} Block=${band.getBlockXSize}x${band.getBlockYSize} ")
      println(s"Type=${band.getDataType}, ColorInterp=${band.getColorInterpretationName}")
      band.getDescription match {
        case Some(description) => println(s"  Description = $description")
        case None =>
      }

      // TODO: min/max
      // TODO: minimum and mean
      // TODO: bucket count
      // TODO: checksum

      band.getNoDataValue match {
        case Some(noDataValue) => println(s"  NoData Value=$noDataValue")
        case None =>
      }

      // TODO: overviews
      // TODO: mask flags
      // TODO: mask band overviews
      // TODO: unit type

      if (band.getRasterCategoryNames.nonEmpty) {
        println( "  Categories:" )
        for ((category, i) <- band.getRasterCategoryNames.zipWithIndex) {
          println(s"      $i: $category")
        }
      }

      // TODO: offset
      // TODO: scale
      // TODO: metadata

      if (band.getColorInterpretationName == "Palette") {
        RasterColor.printableColorTable(band) match {
          case Some((colorTable, name)) =>
            println(s"  Color Table ($name with ${colorTable.size} entries)")
            if (options.showColorTable) {
              for ((color, i) <- colorTable.zipWithIndex) {
                println(s"    $i: $color");
              }
            }
          case None =>
        }
      }

      // TODO: RAT
    }
  }

  def reportCorner(dataset: GDALDataset, cornerName: String, x: Double, y: Double) = {
    print(cornerName + " ")
    if (dataset.getGeoTransform.exists(_ != 0)) {
      println(s"($x,$y)")
    } else {
      val gt = dataset.getGeoTransform
      val geox = gt(0) + gt(1) * x + gt(2) * y
      val geoy = gt(3) + gt(4) * x + gt(5) * y
      print(f"($geox%12.3f,$geoy%12.3f) ")

      dataset.getProjectionRef match {
        case Some(projection) =>
          val srs = new SpatialReference(projection)
          if (srs != null && projection.length != 0) {
            val latLong = srs.CloneGeogCS()
            if (latLong != null) {
              val transform = CoordinateTransformation
                .CreateCoordinateTransformation(srs, latLong)
              if (transform != null) {
                val point = transform.TransformPoint(geox, geoy, 0)
                val xtrans = sgdal.decToDMS(point(0), "Long", 2)
                val ytrans = sgdal.decToDMS(point(1), "Lat", 2)
                println(s"($xtrans,$ytrans)")
                transform.delete()
              }
            }
          }
          if (srs != null) {
            srs.delete()
          }
        case None => println()
      }
    }
  }
}

class RasterColor(color: Color) {
  override def toString: String = s"${color.getRed},${color.getGreen},${color.getBlue},${color.getAlpha}"
}

object RasterColor {
  def printableColorTable(band: GDALBand): Option[(Vector[RasterColor], String)] = {
    Option(band.getRasterColorTable).map { ct =>
      ((0 until ct.getCount).map { i => new RasterColor(ct.getColorEntry(i)) }.toVector, ct.getPaletteInterpretationName)
    }
  }
}

/*
 * Copyright 2019 Azavea
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

package geotrellis

import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent
import geotrellis.vector.Extent

import org.gdal.gdal.{Band, Dataset, gdal => sgdal}
import org.gdal.osr.SpatialReference

import java.security.MessageDigest
import java.util.Base64
import java.util.{Vector => JVector}

import scala.util.Try
import scala.collection.JavaConverters._

package object gdal extends Serializable {
  implicit class withVectorMethods[T](vector: Vector[T]) {
    def asJava: JVector[Any] = {
      val vec = new java.util.Vector[Any]()
      vec.addAll(vector.asJavaCollection)
      vec
    }
  }

  implicit class StringMethods(val str: String) extends AnyVal {
    def base64: String = Base64.getEncoder.encodeToString(str.getBytes)
    def md5: String = MessageDigest.getInstance("MD5").digest(str.getBytes).map("%02x".format(_)).mkString
  }

  implicit class GDALWarpOptionsListMethods(val options: List[GDALWarpOptions]) {
    val name: String = options.map(_.name).mkString("_")
  }

  implicit class GDALWarpOptionsListDependentMethods(options: (GDALWarpOptions, List[GDALWarpOptions])) {
    val name: String = { val (opt, list) = options; s"${opt.name}${list.name}" }
  }

  implicit class GDALWarpOptionsListDatasetDependentMethods(options: (String, List[GDALWarpOptions])) {
    val name: String = { val (path, list) = options; s"${path}${list.name}" }
  }

  implicit class GDALWarpOptionOptionsListDatasetDependentMethods(options: Option[(String, List[GDALWarpOptions])]) {
    val name: String = options.map(_.name).getOrElse("")
  }

  implicit class GDALWarpOptionsOptionMethods(val options: Option[GDALWarpOptions]) {
    val name: String = options.map(_.name).getOrElse("")
  }

  implicit class GDALSpatialReference(self: SpatialReference) {
    def toCRS: CRS = CRS.fromString(self.ExportToProj4)
  }

  implicit class GDALRasterBandMethods(self: Band) {
    def getNoDataValue: Option[Double] = {
      val arr = Array.ofDim[java.lang.Double](1)
      self.GetNoDataValue(arr)
      arr.headOption.flatMap(Option(_)).map(_.doubleValue())
    }

    def computeRasterMinMax(approx_ok: Int): Option[(Double, Double)] = AnyRef.synchronized {
      val arr = Array.ofDim[Double](2)
      self.ComputeRasterMinMax(arr, approx_ok)
      if (arr.length == 2) Some(arr(0) -> arr(1))
      else None
    }

    def computeRasterMinMax: Option[(Double, Double)] = AnyRef.synchronized {
      val arr = Array.ofDim[Double](2)
      self.ComputeRasterMinMax(arr)
      if (arr.length == 2) Some(arr(0) -> arr(1))
      else None
    }

    def computeBandStats(samplestep: Int): Option[(Double, Double)] = AnyRef.synchronized {
      val arr = Array.ofDim[Double](2)
      self.ComputeBandStats(arr, samplestep)
      if (arr.length == 2) Some(arr(0) -> arr(1))
      else None
    }

    def computeBandStats: Option[(Double, Double)] = AnyRef.synchronized {
      val arr = Array.ofDim[Double](2)
      self.ComputeBandStats(arr)
      if (arr.length == 2) Some(arr(0) -> arr(1))
      else None
    }
  }

  implicit class GDALDatasetMethods(self: Dataset) {
    def getRasterBands: List[Band] = (1 until self.GetRasterCount()).map(self.GetRasterBand).toList

    /** GetGeoTransform, OSR objects and all related to it methods are not threadsafe */
    def getProjectionRef: Option[String] = AnyRef.synchronized(Option(self.GetProjectionRef))
    def getProjection: Option[String] = AnyRef.synchronized(Option(self.GetProjection))

    def getNoDataValue: Option[Double] = self.GetRasterBand(1).getNoDataValue

    lazy val extent: Extent = Extent(xmin, ymin, xmax, ymax)

    lazy val rasterExtent: RasterExtent = {
      if(self.GetRasterXSize * self.GetRasterYSize > Int.MaxValue)
        sys.error(s"Cannot read this raster, cols * rows exceeds maximum array index (${self.GetRasterXSize} * ${self.GetRasterYSize})")

      RasterExtent(extent, self.GetRasterXSize, self.GetRasterYSize)
    }

    lazy val gridBounds: GridBounds = GridBounds(0, 0, self.GetRasterXSize - 1, self.GetRasterYSize - 1)

    lazy val geoTransform: Array[Double] = AnyRef.synchronized(self.GetGeoTransform)

    lazy val xmin: Double = geoTransform(0)

    lazy val ymin: Double = geoTransform(3) + geoTransform(5) * self.GetRasterYSize

    lazy val xmax: Double = geoTransform(0) +  geoTransform(1) * self.GetRasterXSize

    lazy val ymax: Double = geoTransform(3)

    lazy val cellSize: CellSize = CellSize(geoTransform(1), math.abs(geoTransform(5)))

    def crs: Option[CRS] = getProjectionRef.flatMap { s => Try { new SpatialReference(s).toCRS }.toOption }

    def cellType: CellType = {
      val baseBand = self.GetRasterBand(1)

      // we don't have access to the sampleFormat, but we can calculate MinMax values
      // would be calculated only to derive UByte cell type
      lazy val minMax = baseBand.computeRasterMinMax

      // sampleFormat
      val bufferType = baseBand.getDataType

      // bits per sample
      val typeSizeInBits = sgdal.GetDataTypeSize(bufferType)

      GDALUtils.dataTypeToCellType(
        datatype       = bufferType,
        noDataValue    = getNoDataValue,
        typeSizeInBits = Some(typeSizeInBits),
        minMaxValues   = minMax
      )
    }
  }

  implicit class CellTypeMethods(self: CellType) {
    def isUnsigned: Boolean = {
      self match {
        case UByteUserDefinedNoDataCellType(_) | UByteConstantNoDataCellType | UByteCellType |
             UShortUserDefinedNoDataCellType(_) | UShortConstantNoDataCellType | UShortCellType  => true
        case _ => false
      }
    }
  }

  implicit class rasterExtentMethods(self: RasterExtent) {
    def reproject(src: CRS, dest: CRS, options: Options): RasterExtent =
      if(src == dest) self
      else {
        val transform = Transform(src, dest)
        options.targetRasterExtent.getOrElse(ReprojectRasterExtent(self, transform, options = options))
      }

    def reproject(src: CRS, dest: CRS): RasterExtent =
      reproject(src, dest, Options.DEFAULT)
  }
}

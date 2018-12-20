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

package geotrellis

import java.util.Base64

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, GridBounds, RasterExtent}
import geotrellis.util.MethodExtensions
import geotrellis.vector.Extent
import org.gdal.osr.SpatialReference

package object gdal extends Serializable {
  implicit class SpatialReferenceMethods(val sr: SpatialReference) extends AnyVal {
    def toCRS: CRS = CRS.fromString(sr.ExportToProj4)
  }

  implicit class StringMethods(val str: String) extends AnyVal {
    def base64: String = Base64.getEncoder.encodeToString(str.getBytes)
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

  implicit class GDALDatasetMethods(val self: GDALDataset) extends MethodExtensions[GDALDataset] {
    lazy val extent: Extent = Extent(xmin, ymin, xmax, ymax)

    lazy val rasterExtent: RasterExtent = {
      if(self.getRasterXSize * self.getRasterYSize > Int.MaxValue)
        sys.error(s"Cannot read this raster, cols * rows exceeds maximum array index (${self.getRasterXSize} * ${self.getRasterYSize})")

      RasterExtent(extent, self.getRasterXSize, self.getRasterYSize)
    }

    lazy val gridBounds: GridBounds = GridBounds(0, 0, self.getRasterXSize - 1, self.getRasterYSize - 1)

    lazy val geoTransform: Array[Double] = self.getGeoTransform

    lazy val xmin: Double = geoTransform(0)

    lazy val ymin: Double = geoTransform(3) + geoTransform(5) * self.getRasterYSize

    lazy val xmax: Double = geoTransform(0) +  geoTransform(1) * self.getRasterXSize

    lazy val ymax: Double = geoTransform(3)

    lazy val cellSize: CellSize = CellSize(geoTransform(1), math.abs(geoTransform(5)))

    def crs: Option[CRS] = self.getProjectionRef map { projection =>
      val srs = new SpatialReference(projection)
      CRS.fromString(srs.ExportToProj4())
    }
  }
}

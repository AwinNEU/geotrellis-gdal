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

package geotrellis.gdal

import geotrellis.gdal.cache.LazyCache
import geotrellis.gdal.config.{GDALCacheConfig, GDALOptionsConfig}

import cats.syntax.foldable._
import cats.syntax.option._
import cats.instances.list._
import cats.instances.either._

import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import com.typesafe.scalalogging.LazyLogging

import java.net.URI

private [gdal] case class GDALException(code: Int, msg: String) extends RuntimeException(s"GDAL ERROR $code: $msg")

private [gdal] object GDALException {
  def lastError(): GDALException = GDALException(gdal.GetLastErrorNo, gdal.GetLastErrorMsg)
}

object GDAL extends LazyLogging {
  // NEL, Dataset is the result VRT and List[Dataset] is a list of parent Datasets
  type HDataset = (Dataset, List[Dataset])

  gdal.AllRegister

  // sets GDALConfig options
  GDALOptionsConfig.set

  def openURI(uri: URI): Dataset =
    openPath(VSIPath(uri.toString).vsiPath)

  def open(path: String): Dataset =
    if (VSIPath.isVSIFormatted(path))
      openPath(path)
    else
      openPath(VSIPath(path).vsiPath)

  @transient lazy val cache: LazyCache[String, Dataset] = GDALCacheConfig.getCache

  /** We may want to force invalidate caches, in case we don't trust GC too much */
  def cacheCleanUp: Unit = cache.invalidateAll()

  def openPath(path: String, nOpenFlags: Long, allowedDrivers: Vector[_], openOptions: Vector[_], siblingFiles: Vector[_]): Dataset =
    openPathDS(path, gdal.OpenEx(path, nOpenFlags, allowedDrivers.asJava, openOptions.asJava, siblingFiles.asJava))

  def openPath(path: String, nOpenFlags: Long, allowedDrivers: Vector[_], openOptions: Vector[_]): Dataset =
    openPathDS(path, gdal.OpenEx(path, nOpenFlags, allowedDrivers.asJava, openOptions.asJava))

  def openPath(path: String, nOpenFlags: Long, allowedDrivers: Vector[_]): Dataset =
    openPathDS(path, gdal.OpenEx(path, nOpenFlags, allowedDrivers.asJava))

  def openPath(path: String, nOpenFlags: Long): Dataset =
    openPathDS(path, gdal.OpenEx(path, nOpenFlags))

  def openPath(path: String): Dataset =
    openPathDS(path, gdal.OpenEx(path, gdalconstConstants.GA_ReadOnly))

  private def openPathDS(path: String, getDS: => Dataset): Dataset = {
    val ds = cache.get(path.md5, _ => getDS)
    if(ds == null) throw GDALException.lastError()
    ds
  }

  // parentWarpOptions is a tuple of a path to the initial dataset and a list of previous transformations
  // it is required to calculate a proper cache key
  private def warp(dest: String, baseDatasets: Array[Dataset], warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): Dataset = {
    // current warp key
    val key = s"${parentWarpOptions.name}${warpOptions.name}".md5
    // put parent into the strong cache
    lazy val getDS = gdal.Warp(dest, baseDatasets, warpOptions.toWarpOptions)
    val ds = cache.get(key.md5, _ => getDS)
    if(ds == null) throw GDALException.lastError()
    ds
  }

  def warp(dest: String, baseDataset: Dataset, warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): Dataset =
    warp(dest, Array(baseDataset), warpOptions, parentWarpOptions)

  def fromGDALWarpOptions(uri: String, list: List[GDALWarpOptions]): Dataset =
    fromGDALWarpOptions(uri, list, GDAL.open(uri))

  def fromGDALWarpOptions(uri: String, list: List[GDALWarpOptions], baseDataset: Dataset): Dataset =
    fromGDALWarpOptionsH(uri, list, baseDataset)._1

  def fromGDALWarpOptionsH(uri: String, list: List[GDALWarpOptions]): HDataset =
    fromGDALWarpOptionsH(uri, list, GDAL.open(uri))

  def fromGDALWarpOptionsH(uri: String, list: List[GDALWarpOptions], baseDataset: Dataset): HDataset = {
    // if we want to perform warp operations
    if (list.nonEmpty) {
      // let's find the latest cached dataset, once we'll find smth let's stop
      val Left(Some(dataset)) =
        list.zipWithIndex.reverse.foldLeftM(Option.empty[HDataset]) { case (acc, (_, idx)) =>
          acc match {
            // successful dataset retrive, in case for some reason there is smth non empty
            case ds @ Some(_) => Left(ds)

            // we haven't read anything
            case None =>
              if (idx == 0) {
                Left(Option(list.zipWithIndex.foldLeft(baseDataset -> List[Dataset](baseDataset)) { case ((ds, dsh), (ops, index)) =>
                  val res = warp("", ds, ops, (uri, list.take(index)).some)
                  val history = if(index != list.length - 1) dsh :+ res else dsh
                  (res, history)
                }))
              } else {
                val key = {
                  val allWarpOptions = list.take(idx + 1)
                  val parentWarpOptions = Some((uri, allWarpOptions.take(idx)))
                  val warpOptions = allWarpOptions.last

                  s"${parentWarpOptions.name}${warpOptions.name}".md5
                }
                val result: Option[HDataset] = cache.getIfPresent(key).map { base =>
                  // build new Datasets that are not in cache
                  val (dataset, newHistory): HDataset =
                    list.drop(idx).foldLeft(base -> List[Dataset](base)) { case ((ds, dsh), ops) =>
                      val res = warp("", ds, ops, (uri, list.drop(idx)).some)
                      (res, dsh :+ res)
                    }

                  // get all the history data
                  val history: List[Dataset] = list.take(idx).zipWithIndex.flatMap { case (_, index) =>
                    cache.getIfPresent((uri, list.take(index + 1).reverse).name.md5)
                  }

                  // drop head to have an expected behavior
                  (dataset, history ::: newHistory.take(newHistory.length - 1))
                }
                if (result.isEmpty) Right(result)
                else Left(result)
              }
          }
        }

      dataset
    } else baseDataset -> List[Dataset]()
  }

  GDALCacheConfig.addShutdownHook
}

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

import geotrellis.gdal.cache.LazyCache
import geotrellis.gdal.config.GDALCacheConfig

import cats.syntax.foldable._
import cats.syntax.option._
import cats.instances.list._
import cats.instances.either._

import org.gdal.gdalconst.gdalconstConstants
import com.typesafe.scalalogging.LazyLogging

import java.net.URI

// All of the logic in this file was taken from:
// https://github.com/geotrellis/geotrellis-gdal/blob/master/gdal/src/main/scala/geotrellis/gdal/Gdal.scala

private [gdal] class GDALException(code: Int, msg: String)
  extends RuntimeException(s"GDAL ERROR $code: $msg")

private [gdal] object GDALException {
  def lastError(): GDALException =
    new GDALException(sgdal.getLastErrorNo, sgdal.getLastErrorMsg)
}

object GDAL extends LazyLogging {
  sgdal.AllRegister

  def openURI(uri: URI): GDALDataset =
    openPath(VSIPath(uri.toString).vsiPath)

  def open(path: String): GDALDataset =
    if (VSIPath.isVSIFormatted(path))
      openPath(path)
    else
      openPath(VSIPath(path).vsiPath)

  @transient lazy val cache: LazyCache[String, GDALDataset] = GDALCacheConfig.getCache

  /** We may want to force invalidate caches, in case we don't trust GC too much */
  def cacheCleanUp: Unit = cache.invalidateAll()

  def openPath(path: String): GDALDataset = {
    lazy val getDS = sgdal.open(path, gdalconstConstants.GA_ReadOnly)
    val ds = cache.get(path.base64, _ => getDS)
    if(ds == null) throw GDALException.lastError()
    ds
  }

  // parentWarpOptions is a tuple of a path to the initial dataset and a list of previous transformations
  // it is required to calculate a proper cache key
  def warp(dest: String, baseDatasets: Array[GDALDataset], warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): GDALDataset = {
    lazy val getDS = sgdal.warp(dest, baseDatasets, warpOptions.toWarpOptions)
    val key = s"${parentWarpOptions.name}${warpOptions.name}".base64
    val ds = cache.get(key.base64, _ => getDS)
    if(ds == null) throw GDALException.lastError()
    ds
  }

  def warp(dest: String, baseDataset: GDALDataset, warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): GDALDataset =
    warp(dest, Array(baseDataset), warpOptions, parentWarpOptions)

  def fromGDALWarpOptions(uri: String, list: List[GDALWarpOptions]): GDALDataset = {
    // if we want to perform warp operations
    if (list.nonEmpty) {
      // let's find the latest cached dataset, once we'll find smth let's stop
      val Left(Some(dataset)) =
        list.zipWithIndex.reverse.foldLeftM(Option.empty[GDALDataset]) { case (acc, (_, idx)) =>
          acc match {
            // successful dataset retrive, in case for some reason there is smth non empty
            case ds @ Some(_) => Left(ds)

            // we haven't read anything
            case None =>
              if (idx == 0) {
                Left(Option(list.zipWithIndex.foldLeft(open(uri)) { case (ds, (ops, index)) =>
                  warp("", ds, ops, (uri, list.take(index)).some)
                }))
              } else {
                val result = cache.getIfPresent((uri, list.take(idx)).name).map { c => list.drop(idx).foldLeft(c) { warp("", _, _, (uri, list.drop(idx)).some) } }
                if (result.isEmpty) Right(result)
                else Left(result)
              }
          }
        }

      dataset
    } else open(uri) // just open a GDAL dataset
  }

  GDALCacheConfig.addShutdownHook
}

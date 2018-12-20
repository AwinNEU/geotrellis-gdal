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

import org.gdal.gdal._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

object sgdal {
  def useExceptions: Unit = AnyRef.synchronized {
    gdal.UseExceptions
  }

  def dontUseExceptions: Unit = AnyRef.synchronized {
    gdal.DontUseExceptions
  }

  def generalCmdLineProcessor(args: Array[String], nOptions: Int): Array[String] = AnyRef.synchronized {
    gdal.GeneralCmdLineProcessor(args, nOptions)
  }

  def generalCmdLineProcessor(args: Array[String]): Array[String] = AnyRef.synchronized {
    gdal.GeneralCmdLineProcessor(args)
  }

  def invGeoTransform(gt_in: Array[Double]): Array[Double] = AnyRef.synchronized {
    gdal.InvGeoTransform(gt_in)
  }

  def debug(msg_class: String, message: String): Unit = AnyRef.synchronized {
    gdal.Debug(msg_class, message)
  }

  def setErrorHandler(pszCallbackName: String): Int = AnyRef.synchronized {
    gdal.SetErrorHandler(pszCallbackName)
  }

  def setErrorHandler: Int = AnyRef.synchronized {
    gdal.SetErrorHandler
  }

  def pushErrorHandler(pszCallbackName: String): Int = AnyRef.synchronized {
    gdal.PushErrorHandler(pszCallbackName)
  }

  def pushErrorHandler: Int = AnyRef.synchronized {
    gdal.PushErrorHandler
  }

  def error(msg_class: Int, err_code: Int, msg: String): Unit = AnyRef.synchronized {
    gdal.Error(msg_class, err_code, msg)
  }

  def GOA2GetAuthorizationURL(pszScope: String): String = AnyRef.synchronized {
    gdal.GOA2GetAuthorizationURL(pszScope)
  }

  def GOA2GetRefreshToken(pszAuthToken: String, pszScope: String): String = AnyRef.synchronized {
    gdal.GOA2GetRefreshToken(pszAuthToken, pszScope)
  }

  def GOA2GetAccessToken(pszRefreshToken: String, pszScope: String): String = AnyRef.synchronized {
    gdal.GOA2GetAccessToken(pszRefreshToken, pszScope)
  }

  def popErrorHandler: Unit = AnyRef.synchronized {
    gdal.PopErrorHandler
  }

  def errorReset: Unit = AnyRef.synchronized {
    gdal.ErrorReset
  }

  def escapeString(len: Array[Byte], scheme: Int): String = AnyRef.synchronized {
    gdal.EscapeString(len, scheme)
  }

  def escapeString(str: String, scheme: Int): String = AnyRef.synchronized {
    gdal.EscapeString(str, scheme)
  }

  def getLastErrorNo: Int = AnyRef.synchronized {
    gdal.GetLastErrorNo
  }

  def getLastErrorType: Int = AnyRef.synchronized {
    gdal.GetLastErrorType
  }

  def getLastErrorMsg: String = AnyRef.synchronized {
    gdal.GetLastErrorMsg
  }

  def getErrorCounter: Long = AnyRef.synchronized {
    gdal.GetErrorCounter
  }

  def VSIGetLastErrorNo: Int = AnyRef.synchronized {
    gdal.VSIGetLastErrorNo()
  }

  def VSIGetLastErrorMsg: String = AnyRef.synchronized {
    gdal.VSIGetLastErrorMsg
  }

  def pushFinderLocation(utf8_path: String): Unit = AnyRef.synchronized {
    gdal.PushFinderLocation(utf8_path)
  }

  def popFinderLocation: Unit = AnyRef.synchronized {
    gdal.PopFinderLocation
  }

  def finderClean(): Unit = AnyRef.synchronized {
    gdal.FinderClean
  }

  def findFile(pszClass: String, utf8_path: String): String = AnyRef.synchronized {
    gdal.FindFile(pszClass, utf8_path)
  }

  def readDir(utf8_path: String, nMaxFiles: Int): Vector[String] = AnyRef.synchronized {
    gdal.ReadDir(utf8_path, nMaxFiles).asScala.toVector.map(_.asInstanceOf[String])
  }

  def readDir(utf8_path: String): Vector[String] = AnyRef.synchronized {
    gdal.ReadDir(utf8_path).asScala.toVector.map(_.asInstanceOf[String])
  }

  def readDirRecursive(utf8_path: String): Vector[String] = AnyRef.synchronized {
    gdal.ReadDirRecursive(utf8_path).asScala.toVector.map(_.asInstanceOf[String])
  }

  def setConfigOption(pszKey: String, pszValue: String): Unit = AnyRef.synchronized {
    gdal.SetConfigOption(pszKey, pszValue)
  }

  def getConfigOption(pszKey: String, pszDefault: String): String = AnyRef.synchronized {
    gdal.GetConfigOption(pszKey, pszDefault)
  }

  def getConfigOption(pszKey: String): String = AnyRef.synchronized {
    gdal.GetConfigOption(pszKey)
  }

  def CPLBinaryToHex(nBytes: Array[Byte]): String = AnyRef.synchronized {
    gdal.CPLBinaryToHex(nBytes)
  }

  def CPLHexToBinary(pszHex: String): Array[Byte] = AnyRef.synchronized {
    gdal.CPLHexToBinary(pszHex)
  }

  def fileFromMemBuffer(utf8_path: String, nBytes: Array[Byte]): Unit = AnyRef.synchronized {
    gdal.FileFromMemBuffer(utf8_path, nBytes)
  }

  def unlink(utf8_path: String): Int = AnyRef.synchronized {
    gdal.Unlink(utf8_path)
  }

  def hasThreadSupport: Int = AnyRef.synchronized {
    gdal.HasThreadSupport
  }

  def mkdir(utf8_path: String, mode: Int): Int = AnyRef.synchronized {
    gdal.Mkdir(utf8_path, mode)
  }

  def rmdir(utf8_path: String): Int = AnyRef.synchronized {
    gdal.Rmdir(utf8_path)
  }

  def mkdirRecursive(utf8_path: String, mode: Int): Int = AnyRef.synchronized {
    gdal.MkdirRecursive(utf8_path, mode)
  }

  def rmdirRecursive(utf8_path: String): Int = AnyRef.synchronized {
    gdal.RmdirRecursive(utf8_path)
  }

  def rename(pszOld: String, pszNew: String): Int = AnyRef.synchronized {
    gdal.Rename(pszOld, pszNew)
  }

  def getActualURL(utf8_path: String): String = AnyRef.synchronized {
    gdal.GetActualURL(utf8_path)
  }

  def getSignedURL(utf8_path: String, options: Vector[_]): String = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.GetSignedURL(utf8_path, vector)
  }

  def getSignedURL(utf8_path: String): String = AnyRef.synchronized {
    gdal.GetSignedURL(utf8_path)
  }

  def getFileSystemsPrefixes: Vector[String] = AnyRef.synchronized {
    gdal.GetFileSystemsPrefixes.asScala.toVector.map(_.asInstanceOf[String])
  }

  def getFileSystemOptions(utf8_path: String): String = AnyRef.synchronized {
    gdal.GetFileSystemOptions(utf8_path)
  }

  def parseCommandLine(utf8_path: String): Vector[String] = AnyRef.synchronized {
    gdal.ParseCommandLine(utf8_path).asScala.toVector.map(_.asInstanceOf[String])
  }

  def GDAL_GCP_GCPX_get(gcp: GCP): Double = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPX_get(gcp)
  }

  def GDAL_GCP_GCPX_set(gcp: GCP, dfGCPX: Double): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPX_set(gcp, dfGCPX)
  }

  def GDAL_GCP_GCPY_get(gcp: GCP): Double = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPY_get(gcp)
  }

  def GDAL_GCP_GCPY_set(gcp: GCP, dfGCPY: Double): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPY_set(gcp, dfGCPY)
  }

  def GDAL_GCP_GCPZ_get(gcp: GCP): Double = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPZ_get(gcp)
  }

  def GDAL_GCP_GCPZ_set(gcp: GCP, dfGCPZ: Double): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPZ_set(gcp, dfGCPZ)
  }

  def GDAL_GCP_GCPPixel_get(gcp: GCP): Double = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPPixel_get(gcp)
  }

  def GDAL_GCP_GCPPixel_set(gcp: GCP, dfGCPPixel: Double): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPPixel_set(gcp, dfGCPPixel)
  }

  def GDAL_GCP_GCPLine_get(gcp: GCP): Double = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPLine_get(gcp)
  }

  def GDAL_GCP_GCPLine_set(gcp: GCP, dfGCPLine: Double): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_GCPLine_set(gcp, dfGCPLine)
  }

  def GDAL_GCP_Info_get(gcp: GCP): String = AnyRef.synchronized {
    gdal.GDAL_GCP_Info_get(gcp)
  }

  def GDAL_GCP_Info_set(gcp: GCP, pszInfo: String): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_Info_set(gcp, pszInfo)
  }

  def GDAL_GCP_Id_get(gcp: GCP): String = AnyRef.synchronized {
    gdal.GDAL_GCP_Id_get(gcp)
  }

  def GDAL_GCP_Id_set(gcp: GCP, pszId: String): Unit = AnyRef.synchronized {
    gdal.GDAL_GCP_Id_set(gcp, pszId)
  }

  def GCPsToGeoTransform(nGCPs: Array[GCP], argout: Array[Double], bApproxOK: Int): Int = AnyRef.synchronized {
    gdal.GCPsToGeoTransform(nGCPs, argout, bApproxOK)
  }

  def GCPsToGeoTransform(nGCPs: Array[GCP], argout: Array[Double]): Int = AnyRef.synchronized {
    gdal.GCPsToGeoTransform(nGCPs, argout)
  }

  def computeMedianCutPCT(red: GDALBand, green: GDALBand, blue: GDALBand, num_colors: Int, colors: GDALColorTable, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.ComputeMedianCutPCT(red.underlying, green.underlying, blue.underlying, num_colors, colors.underlying, callback)
  }

  def computeMedianCutPCT(red: GDALBand, green: GDALBand, blue: GDALBand, num_colors: Int, colors: GDALColorTable): Int = AnyRef.synchronized {
    gdal.ComputeMedianCutPCT(red.underlying, green.underlying, blue.underlying, num_colors, colors.underlying)
  }

  def ditherRGB2PCT(red: GDALBand, green: GDALBand, blue: GDALBand, target: GDALBand, colors: GDALColorTable, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.DitherRGB2PCT(red.underlying, green.underlying, blue.underlying, target.underlying, colors.underlying, callback)
  }

  def ditherRGB2PCT(red: GDALBand, green: GDALBand, blue: GDALBand, target: GDALBand, colors: GDALColorTable): Int = AnyRef.synchronized {
    gdal.DitherRGB2PCT(red.underlying, green.underlying, blue.underlying, target.underlying, colors.underlying)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int, WarpMemoryLimit: Double, maxerror: Double, callback: ProgressCallback, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt, eResampleAlg, WarpMemoryLimit, maxerror, callback, vector)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int, WarpMemoryLimit: Double, maxerror: Double, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt, eResampleAlg, WarpMemoryLimit, maxerror, callback)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int, WarpMemoryLimit: Double, maxerror: Double): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt, eResampleAlg, WarpMemoryLimit, maxerror)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int, WarpMemoryLimit: Double): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt, eResampleAlg, WarpMemoryLimit)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt, eResampleAlg)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String, dst_wkt: String): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt, dst_wkt)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset, src_wkt: String): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying, src_wkt)
  }

  def reprojectImage(src_ds: GDALDataset, dst_ds: GDALDataset): Int = AnyRef.synchronized {
    gdal.ReprojectImage(src_ds.underlying, dst_ds.underlying)
  }

  def computeProximity(srcBand: GDALBand, proximityBand: GDALBand, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.ComputeProximity(srcBand.underlying, proximityBand.underlying, vector, callback)
  }

  def computeProximity(srcBand: GDALBand, proximityBand: GDALBand, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.ComputeProximity(srcBand.underlying, proximityBand.underlying, vector)
  }

  def computeProximity(srcBand: GDALBand, proximityBand: GDALBand): Int = AnyRef.synchronized {
    gdal.ComputeProximity(srcBand.underlying, proximityBand.underlying)
  }

  def rasterizeLayer(dataset: GDALDataset, bands: Array[Int], layer: GDALLayer, burn_values: Array[Double], options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.RasterizeLayer(dataset.underlying, bands, layer.underlying, burn_values, vector, callback)
  }

  def rasterizeLayer(dataset: GDALDataset, bands: Array[Int], layer: GDALLayer, burn_values: Array[Double], options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.RasterizeLayer(dataset.underlying, bands, layer.underlying, burn_values, vector)
  }

  def rasterizeLayer(dataset: GDALDataset, bands: Array[Int], layer: GDALLayer, burn_values: Array[Double]): Int = AnyRef.synchronized {
    gdal.RasterizeLayer(dataset.underlying, bands, layer.underlying, burn_values)
  }

  def rasterizeLayer(dataset: GDALDataset, bands: Array[Int], layer: GDALLayer): Int = AnyRef.synchronized {
    gdal.RasterizeLayer(dataset.underlying, bands, layer.underlying)
  }

  def polygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.Polygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField, vector, callback)
  }

  def polygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.Polygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField, vector)
  }

  def polygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int): Int = AnyRef.synchronized {
    gdal.Polygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField)
  }

  def fPolygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.FPolygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField, vector, callback)
  }

  def fPolygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.FPolygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField, vector)
  }

  def fPolygonize(srcBand: GDALBand, maskBand: GDALBand, outLayer: GDALLayer, iPixValField: Int): Int = AnyRef.synchronized {
    gdal.FPolygonize(srcBand.underlying, maskBand.underlying, outLayer.underlying, iPixValField)
  }

  def fillNodata(targetBand: GDALBand, maskBand: GDALBand, maxSearchDist: Double, smoothingIterations: Int, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.FillNodata(targetBand.underlying, maskBand.underlying, maxSearchDist, smoothingIterations, vector, callback)
  }

  def fillNodata(targetBand: GDALBand, maskBand: GDALBand, maxSearchDist: Double, smoothingIterations: Int, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.FillNodata(targetBand.underlying, maskBand.underlying, maxSearchDist, smoothingIterations, vector)
  }

  def fillNodata(targetBand: GDALBand, maskBand: GDALBand, maxSearchDist: Double, smoothingIterations: Int): Int = AnyRef.synchronized {
    gdal.FillNodata(targetBand.underlying, maskBand.underlying, maxSearchDist, smoothingIterations)
  }

  def sieveFilter(srcBand: GDALBand, maskBand: GDALBand, dstBand: GDALBand, threshold: Int, connectedness: Int, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.SieveFilter(srcBand.underlying, maskBand.underlying, dstBand.underlying, threshold,connectedness, vector, callback)
  }

  def sieveFilter(srcBand: GDALBand, maskBand: GDALBand, dstBand: GDALBand, threshold: Int, connectedness: Int, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.SieveFilter(srcBand.underlying, maskBand.underlying, dstBand.underlying, threshold,connectedness, vector)
  }

  def sieveFilter(srcBand: GDALBand, maskBand: GDALBand, dstBand: GDALBand, threshold: Int, connectedness: Int): Int = AnyRef.synchronized {
    gdal.SieveFilter(srcBand.underlying, maskBand.underlying, dstBand.underlying, threshold,connectedness)
  }

  def sieveFilter(srcBand: GDALBand, maskBand: GDALBand, dstBand: GDALBand, threshold: Int): Int = AnyRef.synchronized {
    gdal.SieveFilter(srcBand.underlying, maskBand.underlying, dstBand.underlying, threshold)
  }

  def regenerateOverviews(srcBand: GDALBand, overviewBandCount: Array[GDALBand], resampling: String, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.RegenerateOverviews(srcBand.underlying, overviewBandCount.map(_.underlying), resampling, callback)
  }

  def regenerateOverviews(srcBand: GDALBand, overviewBandCount: Array[GDALBand], resampling: String): Int = AnyRef.synchronized {
    gdal.RegenerateOverviews(srcBand.underlying, overviewBandCount.map(_.underlying), resampling)
  }

  def regenerateOverviews(srcBand: GDALBand, overviewBandCount: Array[GDALBand]): Int = AnyRef.synchronized {
    gdal.RegenerateOverviews(srcBand.underlying, overviewBandCount.map(_.underlying))
  }

  def regenerateOverview(srcBand: GDALBand, overviewBand: GDALBand, resampling: String, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.RegenerateOverview(srcBand.underlying, overviewBand.underlying, resampling, callback)
  }

  def regenerateOverview(srcBand: GDALBand, overviewBand: GDALBand, resampling: String): Int = AnyRef.synchronized {
    gdal.RegenerateOverview(srcBand.underlying, overviewBand.underlying, resampling)
  }

  def regenerateOverview(srcBand: GDALBand, overviewBand: GDALBand): Int = AnyRef.synchronized {
    gdal.RegenerateOverview(srcBand.underlying, overviewBand.underlying)
  }

  def gridCreate(algorithmOptions: String, points: Array[Array[Double]], xMin: Double, xMax: Double, yMin: Double, yMax: Double, xSize: Int, ySize: Int, dataType: Int, nioBuffer: ByteBuffer, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.GridCreate(algorithmOptions, points, xMin, xMax, yMin, yMax, xSize, ySize, dataType, nioBuffer, callback)
  }

  def gridCreate(algorithmOptions: String, points: Array[Array[Double]], xMin: Double, xMax: Double, yMin: Double, yMax: Double, xSize: Int, ySize: Int, dataType: Int, nioBuffer: ByteBuffer): Int = AnyRef.synchronized {
    gdal.GridCreate(algorithmOptions, points, xMin, xMax, yMin, yMax, xSize, ySize, dataType, nioBuffer)
  }

  def contourGenerate(srcBand: GDALBand, contourInterval: Double, contourBase: Double, fixedLevelCount: Array[Double], useNoData: Int, noDataValue: Double, dstLayer: GDALLayer, idField: Int, elevField: Int, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.ContourGenerate(srcBand.underlying, contourInterval, contourBase, fixedLevelCount, useNoData, noDataValue, dstLayer.underlying, idField, elevField, callback)
  }

  def contourGenerate(srcBand: GDALBand, contourInterval: Double, contourBase: Double, fixedLevelCount: Array[Double], useNoData: Int, noDataValue: Double, dstLayer: GDALLayer, idField: Int, elevField: Int): Int = AnyRef.synchronized {
    gdal.ContourGenerate(srcBand.underlying, contourInterval, contourBase, fixedLevelCount, useNoData, noDataValue, dstLayer.underlying, idField, elevField)
  }

  def autoCreateWarpedVRT(src_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int, maxerror: Double): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.AutoCreateWarpedVRT(src_ds.underlying, src_wkt, dst_wkt, eResampleAlg, maxerror))
  }

  def autoCreateWarpedVRT(src_ds: GDALDataset, src_wkt: String, dst_wkt: String, eResampleAlg: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.AutoCreateWarpedVRT(src_ds.underlying, src_wkt, dst_wkt, eResampleAlg))
  }
  
  def autoCreateWarpedVRT(src_ds: GDALDataset, src_wkt: String, dst_wkt: String): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.AutoCreateWarpedVRT(src_ds.underlying, src_wkt, dst_wkt))
  }

  def autoCreateWarpedVRT(src_ds: GDALDataset, src_wkt: String): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.AutoCreateWarpedVRT(src_ds.underlying, src_wkt))
  }

  def autoCreateWarpedVRT(src_ds: GDALDataset): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.AutoCreateWarpedVRT(src_ds.underlying))
  }

  def createPansharpenedVRT(pszXML: String, panchroBand: GDALBand, nInputSpectralBands: Array[GDALBand]): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.CreatePansharpenedVRT(pszXML, panchroBand.underlying, nInputSpectralBands.map(_.underlying)))
  }

  def applyVerticalShiftGrid(src_ds: GDALDataset, grid_ds: GDALDataset, inverse: Boolean, srcUnitToMeter: Double, dstUnitToMeter: Double, options: Vector[_]): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALDataset(gdal.ApplyVerticalShiftGrid(src_ds.underlying, grid_ds.underlying, inverse, srcUnitToMeter, dstUnitToMeter, vector))
  }

  def applyVerticalShiftGrid(src_ds: GDALDataset, grid_ds: GDALDataset, inverse: Boolean, srcUnitToMeter: Double, dstUnitToMeter: Double): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.ApplyVerticalShiftGrid(src_ds.underlying, grid_ds.underlying, inverse, srcUnitToMeter, dstUnitToMeter))
  }

  def applyVerticalShiftGrid(src_ds: GDALDataset, grid_ds: GDALDataset, inverse: Boolean, srcUnitToMeter: Double): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.ApplyVerticalShiftGrid(src_ds.underlying, grid_ds.underlying, inverse, srcUnitToMeter))
  }

  def applyVerticalShiftGrid(src_ds: GDALDataset, grid_ds: GDALDataset, inverse: Boolean): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.ApplyVerticalShiftGrid(src_ds.underlying, grid_ds.underlying, inverse))
  }

  def applyVerticalShiftGrid(src_ds: GDALDataset, grid_ds: GDALDataset): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.ApplyVerticalShiftGrid(src_ds.underlying, grid_ds.underlying))
  }

  def applyGeoTransform(padfGeoTransform: Array[Double], dfPixel: Double, dfLine: Double, pdfGeoX: Array[Double], pdfGeoY: Array[Double]): Unit = AnyRef.synchronized {
    gdal.ApplyGeoTransform(padfGeoTransform, dfPixel, dfLine, pdfGeoX, pdfGeoY)
  }

  def invGeoTransform(gt_in: Array[Double], gt_out: Array[Double]): Int = AnyRef.synchronized {
    gdal.InvGeoTransform(gt_in, gt_out)
  }

  def versionInfo(request: String): String = AnyRef.synchronized {
    gdal.VersionInfo(request)
  }

  def versionInfo: String = AnyRef.synchronized {
    gdal.VersionInfo
  }

  def AllRegister: Unit = AnyRef.synchronized {
    gdal.AllRegister
  }

  def GDALDestroyDriverManager(): Unit = AnyRef.synchronized {
    gdal.GDALDestroyDriverManager
  }

  def getCacheMax: Int = AnyRef.synchronized {
    gdal.GetCacheMax
  }

  def getCacheUsed: Int = AnyRef.synchronized {
    gdal.GetCacheUsed
  }

  def setCacheMax(nBytes: Int): Unit = AnyRef.synchronized {
    gdal.SetCacheMax(nBytes)
  }

  def getDataTypeSize(eDataType: Int): Int = AnyRef.synchronized {
    gdal.GetDataTypeSize(eDataType)
  }

  def dataTypeIsComplex(eDataType: Int): Int = AnyRef.synchronized {
    gdal.DataTypeIsComplex(eDataType)
  }

  def getDataTypeName(eDataType: Int): String = AnyRef.synchronized {
    gdal.GetDataTypeName(eDataType)
  }

  def getDataTypeByName(pszDataTypeName: String): Int = AnyRef.synchronized {
    gdal.GetDataTypeByName(pszDataTypeName)
  }

  def getColorInterpretationName(eColorInterp: Int): String = AnyRef.synchronized {
    gdal.GetColorInterpretationName(eColorInterp)
  }

  def getPaletteInterpretationName(ePaletteInterp: Int): String = AnyRef.synchronized {
    gdal.GetPaletteInterpretationName(ePaletteInterp)
  }

  def decToDMS(dfAngle: Double, pszAxis: String, nPrecision: Int): String = AnyRef.synchronized {
    gdal.DecToDMS(dfAngle, pszAxis, nPrecision)
  }

  def decToDMS(dfAngle: Double, pszAxis: String): String = AnyRef.synchronized {
    gdal.DecToDMS(dfAngle, pszAxis)
  }

  def packedDMSToDec(dfPacked: Double): Double = AnyRef.synchronized {
    gdal.PackedDMSToDec(dfPacked)
  }

  def decToPackedDMS(dfDec: Double): Double = AnyRef.synchronized {
    gdal.DecToPackedDMS(dfDec)
  }

  def parseXMLString(pszXMLString: String): XMLNode = AnyRef.synchronized {
    gdal.ParseXMLString(pszXMLString)
  }

  def serializeXMLTree(xmlnode: XMLNode): String = AnyRef.synchronized {
    gdal.SerializeXMLTree(xmlnode)
  }

  def getJPEG2000StructureAsString(pszFilename: String): String = AnyRef.synchronized {
    gdal.GetJPEG2000StructureAsString(pszFilename)
  }

  def getJPEG2000StructureAsString(pszFilename: String, options: Vector[_]): String = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    gdal.GetJPEG2000StructureAsString(pszFilename, vector)
  }

  def getDriverCount: Int = AnyRef.synchronized {
    gdal.GetDriverCount
  }

  def getDriverByName(name: String): GDALDriver = AnyRef.synchronized {
    GDALDriver(gdal.GetDriverByName(name))
  }

  def getDriver(i: Int): GDALDriver = AnyRef.synchronized {
    GDALDriver(gdal.GetDriver(i))
  }

  def open(utf8_path: String, eAccess: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Open(utf8_path, eAccess))
  }

  def open(name: String): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Open(name))
  }

  def openEx(utf8_path: String, nOpenFlags: Long, allowed_drivers: Vector[_], open_options: Vector[_], sibling_files: Vector[_]): GDALDataset = AnyRef.synchronized {
    val ad = new java.util.Vector[Any]()
    ad.addAll(allowed_drivers.asJavaCollection)

    val oo = new java.util.Vector[Any]()
    oo.addAll(open_options.asJavaCollection)

    val sf = new java.util.Vector[Any]()
    sf.addAll(sibling_files.asJavaCollection)

    GDALDataset(gdal.OpenEx(utf8_path, nOpenFlags, ad, oo, sf))
  }

  def openEx(utf8_path: String, nOpenFlags: Long, allowed_drivers: Vector[_], open_options: Vector[_]): GDALDataset = AnyRef.synchronized {
    val ad = new java.util.Vector[Any]()
    ad.addAll(allowed_drivers.asJavaCollection)

    val oo = new java.util.Vector[Any]()
    oo.addAll(open_options.asJavaCollection)

    GDALDataset(gdal.OpenEx(utf8_path, nOpenFlags, ad, oo))
  }

  def openEx(utf8_path: String, nOpenFlags: Long, allowed_drivers: Vector[_]): GDALDataset = AnyRef.synchronized {
    val ad = new java.util.Vector[Any]()
    ad.addAll(allowed_drivers.asJavaCollection)

    GDALDataset(gdal.OpenEx(utf8_path, nOpenFlags, ad))
  }

  def openEx(utf8_path: String, nOpenFlags: Long): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.OpenEx(utf8_path, nOpenFlags))
  }

  def openEx(utf8_path: String): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.OpenEx(utf8_path))
  }

  def openShared(utf8_path: String, eAccess: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.OpenShared(utf8_path, eAccess))
  }

  def openShared(utf8_path: String): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.OpenShared(utf8_path))
  }

  def identifyDriver(utf8_path: String, papszSiblings: Vector[_]): GDALDriver = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(papszSiblings.asJavaCollection)

    GDALDriver(gdal.IdentifyDriver(utf8_path, vector))
  }

  def identifyDriver(utf8_path: String): GDALDriver = AnyRef.synchronized {
    GDALDriver(gdal.IdentifyDriver(utf8_path))
  }

  def identifyDriverEx(utf8_path: String, nIdentifyFlags: Long, allowed_drivers: Vector[_], sibling_files: Vector[_]): GDALDriver = AnyRef.synchronized {
    val ad = new java.util.Vector[Any]()
    ad.addAll(allowed_drivers.asJavaCollection)

    val sf = new java.util.Vector[Any]()
    sf.addAll(sibling_files.asJavaCollection)

    GDALDriver(gdal.IdentifyDriverEx(utf8_path, nIdentifyFlags, ad, sf))
  }

  def identifyDriverEx(utf8_path: String, nIdentifyFlags: Long, allowed_drivers: Vector[_]): GDALDriver = AnyRef.synchronized {
    val ad = new java.util.Vector[Any]()
    ad.addAll(allowed_drivers.asJavaCollection)

    GDALDriver(gdal.IdentifyDriverEx(utf8_path, nIdentifyFlags, ad))
  }

  def identifyDriverEx(utf8_path: String, nIdentifyFlags: Long): GDALDriver = AnyRef.synchronized {
    GDALDriver(gdal.IdentifyDriverEx(utf8_path, nIdentifyFlags))
  }

  def identifyDriverEx(utf8_path: String): GDALDriver = AnyRef.synchronized {
    GDALDriver(gdal.IdentifyDriverEx(utf8_path))
  }

  def generalCmdLineProcessor(papszArgv: Vector[_], nOptions: Int): Vector[String] = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(papszArgv.asJavaCollection)

    gdal.GeneralCmdLineProcessor(vector, nOptions).asScala.toVector.map(_.asInstanceOf[String])
  }

  def generalCmdLineProcessor(papszArgv: Vector[_]): Vector[String] = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(papszArgv.asJavaCollection)

    gdal.GeneralCmdLineProcessor(vector).asScala.toVector.map(_.asInstanceOf[String])
  }

  def GDALInfo(hDataset: GDALDataset, infoOptions: InfoOptions): String = AnyRef.synchronized {
    gdal.GDALInfo(hDataset.underlying, infoOptions)
  }

  def translate(dest: String, dataset: GDALDataset, translateOptions: TranslateOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Translate(dest, dataset.underlying, translateOptions, callback))
  }

  def translate(dest: String, dataset: GDALDataset, translateOptions: TranslateOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Translate(dest, dataset.underlying, translateOptions))
  }

  def warp(dstDS: GDALDataset, object_list_count: Array[GDALDataset], warpAppOptions: WarpOptions, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.Warp(dstDS.underlying, object_list_count.map(_.underlying), warpAppOptions, callback)
  }

  def warp(dstDS: GDALDataset, object_list_count: Array[GDALDataset], warpAppOptions: WarpOptions): Int = AnyRef.synchronized {
    gdal.Warp(dstDS.underlying, object_list_count.map(_.underlying), warpAppOptions)
  }

  def warp(dest: String, object_list_count: Array[GDALDataset], warpAppOptions: WarpOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Warp(dest, object_list_count.map(_.underlying), warpAppOptions, callback))
  }

  def warp(dest: String, object_list_count: Array[GDALDataset], warpAppOptions: WarpOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Warp(dest, object_list_count.map(_.underlying), warpAppOptions))
  }

  def vectorTranslate(dstDS: GDALDataset, srcDS: GDALDataset, options: VectorTranslateOptions, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.VectorTranslate(dstDS.underlying, srcDS.underlying, options, callback)
  }

  def vectorTranslate(dstDS: GDALDataset, srcDS: GDALDataset, options: VectorTranslateOptions): Int = AnyRef.synchronized {
    gdal.VectorTranslate(dstDS.underlying, srcDS.underlying, options)
  }

  def vectorTranslate(dest: String, srcDS: GDALDataset, options: VectorTranslateOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.VectorTranslate(dest, srcDS.underlying, options, callback))
  }

  def vectorTranslate(dest: String, srcDS: GDALDataset, options: VectorTranslateOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.VectorTranslate(dest, srcDS.underlying, options))
  }

  def DEMProcessing(dest: String, dataset: GDALDataset, pszProcessing: String, pszColorFilename: String, options: DEMProcessingOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.DEMProcessing(dest, dataset.underlying, pszProcessing, pszColorFilename, options, callback))
  }

  def DEMProcessing(dest: String, dataset: GDALDataset, pszProcessing: String, pszColorFilename: String, options: DEMProcessingOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.DEMProcessing(dest, dataset.underlying, pszProcessing, pszColorFilename, options))
  }

  def nearblack(dstDS: GDALDataset, srcDS: GDALDataset, options: NearblackOptions, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.Nearblack(dstDS.underlying, srcDS.underlying, options, callback)
  }

  def nearblack(dstDS: GDALDataset, srcDS: GDALDataset, options: NearblackOptions): Int = AnyRef.synchronized {
    gdal.Nearblack(dstDS.underlying, srcDS.underlying, options)
  }

  def nearblack(dest: String, srcDS: GDALDataset, options: NearblackOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Nearblack(dest, srcDS.underlying, options, callback))
  }

  def nearblack(dest: String, srcDS: GDALDataset, options: NearblackOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Nearblack(dest, srcDS.underlying, options))
  }

  def grid(dest: String, dataset: GDALDataset, options: GridOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Grid(dest, dataset.underlying, options, callback))
  }

  def grid(dest: String, dataset: GDALDataset, options: GridOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Grid(dest, dataset.underlying, options))
  }

  def rasterize(dstDS: GDALDataset, srcDS: GDALDataset, options: RasterizeOptions, callback: ProgressCallback): Int = AnyRef.synchronized {
    gdal.Rasterize(dstDS.underlying, srcDS.underlying, options, callback)
  }

  def rasterize(dstDS: GDALDataset, srcDS: GDALDataset, options: RasterizeOptions): Int = AnyRef.synchronized {
    gdal.Rasterize(dstDS.underlying, srcDS.underlying, options)
  }

  def rasterize(dest: String, srcDS: GDALDataset, options: RasterizeOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Rasterize(dest, srcDS.underlying, options, callback))
  }

  def rasterize(dest: String, srcDS: GDALDataset, options: RasterizeOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.Rasterize(dest, srcDS.underlying, options))
  }

  def buildVRT(dest: String, object_list_count: Array[GDALDataset], options: BuildVRTOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.BuildVRT(dest, object_list_count.map(_.underlying), options, callback))
  }

  def buildVRT(dest: String, object_list_count: Array[GDALDataset], options: BuildVRTOptions): GDALDataset = AnyRef.synchronized {
    GDALDataset(gdal.BuildVRT(dest, object_list_count.map(_.underlying), options))
  }

  def buildVRT(dest: String, source_filenames: Vector[_], options: BuildVRTOptions, callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(source_filenames.asJavaCollection)
    GDALDataset(gdal.BuildVRT(dest, vector, options, callback))
  }

  def buildVRT(dest: String, source_filenames: Vector[_], options: BuildVRTOptions): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(source_filenames.asJavaCollection)
    GDALDataset(gdal.BuildVRT(dest, vector, options))
  }
}

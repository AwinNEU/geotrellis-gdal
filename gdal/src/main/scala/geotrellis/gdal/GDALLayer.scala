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

import org.gdal.ogr._
import org.gdal.osr.SpatialReference

import scala.collection.JavaConverters._

case class GDALLayer(underlying: Layer) {
  def getExtent(force: Boolean): Array[Double] = AnyRef.synchronized {
    underlying.GetExtent(force)
  }

  def getExtent: Array[Double] = AnyRef.synchronized {
    underlying.GetExtent
  }

  def getRefCount: Int = AnyRef.synchronized {
    underlying.GetRefCount
  }

  def setSpatialFilter(filter: Geometry): Unit = AnyRef.synchronized {
    underlying.SetSpatialFilter(filter)
  }

  def setSpatialFilterRect(minx: Double, miny: Double, maxx: Double, maxy: Double): Unit = AnyRef.synchronized {
    underlying.SetSpatialFilterRect(minx, miny, maxx, maxy)
  }

  def setSpatialFilter(iGeomField: Int, filter: Geometry): Unit = AnyRef.synchronized {
    underlying.SetSpatialFilter(iGeomField, filter)
  }

  def setSpatialFilterRect(iGeomField: Int, minx: Double, miny: Double, maxx: Double, maxy: Double): Unit = AnyRef.synchronized {
    underlying.SetSpatialFilterRect(iGeomField, minx, miny, maxx, maxy)
  }

  def getSpatialFilter: Geometry = AnyRef.synchronized {
    underlying.GetSpatialFilter
  }

  def setAttributeFilter(filter_string: String): Int = AnyRef.synchronized {
    underlying.SetAttributeFilter(filter_string)
  }

  def resetReading(): Unit = AnyRef.synchronized {
    underlying.ResetReading
  }

  def getName: String = AnyRef.synchronized {
    underlying.GetName
  }

  def getGeomType: Int = AnyRef.synchronized {
    underlying.GetGeomType
  }

  def getGeometryColumn: String = AnyRef.synchronized {
    underlying.GetGeometryColumn
  }

  def getFIDColumn: String = AnyRef.synchronized {
    underlying.GetFIDColumn
  }

  def getFeature(fid: Long): Feature = AnyRef.synchronized {
    underlying.GetFeature(fid)
  }

  def getNextFeature: Feature = AnyRef.synchronized {
    underlying.GetNextFeature
  }

  def setNextByIndex(new_index: Long): Int = AnyRef.synchronized {
    underlying.SetNextByIndex(new_index)
  }

  def setFeature(feature: Feature): Int = AnyRef.synchronized {
    underlying.SetFeature(feature)
  }

  def createFeature(feature: Feature): Int = AnyRef.synchronized {
    underlying.CreateFeature(feature)
  }

  def deleteFeature(fid: Long): Int = AnyRef.synchronized {
    underlying.DeleteFeature(fid)
  }

  def syncToDisk: Int = AnyRef.synchronized {
    underlying.SyncToDisk
  }

  def getLayerDefn: FeatureDefn = AnyRef.synchronized {
    underlying.GetLayerDefn
  }

  def getFeatureCount(force: Int): Long = AnyRef.synchronized {
    underlying.GetFeatureCount(force)
  }

  def getFeatureCount: Long = AnyRef.synchronized {
    underlying.GetFeatureCount
  }

  def getExtent(argout: Array[Double], force: Int): Int = AnyRef.synchronized {
    underlying.GetExtent(argout, force)
  }

  def testCapability(cap: String): Boolean = AnyRef.synchronized {
    underlying.TestCapability(cap)
  }

  def createField(field_def: FieldDefn, approx_ok: Int): Int = AnyRef.synchronized {
    underlying.CreateField(field_def, approx_ok)
  }

  def createField(field_def: FieldDefn): Int = AnyRef.synchronized {
    underlying.CreateField(field_def)
  }

  def deleteField(iField: Int): Int = AnyRef.synchronized {
    underlying.DeleteField(iField)
  }

  def reorderField(iOldFieldPos: Int, iNewFieldPos: Int): Int = AnyRef.synchronized {
    underlying.ReorderField(iOldFieldPos, iNewFieldPos)
  }

  def reorderFields(nList: Array[Int]): Int = AnyRef.synchronized {
    underlying.ReorderFields(nList)
  }

  def alterFieldDefn(iField: Int, field_def: FieldDefn, nFlags: Int): Int = AnyRef.synchronized {
    underlying.AlterFieldDefn(iField, field_def, nFlags)
  }

  def createGeomField(field_def: GeomFieldDefn, approx_ok: Int): Int = AnyRef.synchronized {
    underlying.CreateGeomField(field_def, approx_ok)
  }

  def createGeomField(field_def: GeomFieldDefn): Int = AnyRef.synchronized {
    underlying.CreateGeomField(field_def)
  }

  def startTransaction: Int = AnyRef.synchronized {
    underlying.StartTransaction
  }

  def commitTransaction: Int = AnyRef.synchronized {
    underlying.CommitTransaction
  }

  def rollbackTransaction: Int = AnyRef.synchronized {
    underlying.RollbackTransaction
  }

  def findFieldIndex(pszFieldName: String, bExactMatch: Int): Int = AnyRef.synchronized {
    underlying.FindFieldIndex(pszFieldName, bExactMatch)
  }

  def getSpatialRef: SpatialReference = AnyRef.synchronized {
    underlying.GetSpatialRef
  }

  def getFeaturesRead: Long = AnyRef.synchronized {
    underlying.GetFeaturesRead
  }

  def setIgnoredFields(options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.SetIgnoredFields(vector)
  }

  def intersection(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Intersection(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def intersection(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Intersection(method_layer.underlying, result_layer.underlying, vector)
  }

  def intersection(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Intersection(method_layer.underlying, result_layer.underlying)
  }

  def union(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Union(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def union(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Union(method_layer.underlying, result_layer.underlying, vector)
  }

  def union(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Union(method_layer.underlying, result_layer.underlying)
  }

  def symDifference(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.SymDifference(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def symDifference(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.SymDifference(method_layer.underlying, result_layer.underlying, vector)
  }

  def symDifference(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.SymDifference(method_layer.underlying, result_layer.underlying)
  }

  def identity(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Identity(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def identity(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Identity(method_layer.underlying, result_layer.underlying, vector)
  }

  def identity(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Identity(method_layer.underlying, result_layer.underlying)
  }

  def update(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Update(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def update(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Update(method_layer.underlying, result_layer.underlying, vector)
  }

  def update(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Update(method_layer.underlying, result_layer.underlying)
  }

  def clip(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Clip(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def clip(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Clip(method_layer.underlying, result_layer.underlying, vector)
  }

  def clip(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Clip(method_layer.underlying, result_layer.underlying)
  }

  def erase(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_], callback: ProgressCallback): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Erase(method_layer.underlying, result_layer.underlying, vector, callback)
  }

  def erase(method_layer: GDALLayer, result_layer: GDALLayer, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.Erase(method_layer.underlying, result_layer.underlying, vector)
  }

  def erase(method_layer: GDALLayer, result_layer: GDALLayer): Int = AnyRef.synchronized {
    underlying.Erase(method_layer.underlying, result_layer.underlying)
  }

  def getStyleTable: GDALStyleTable = AnyRef.synchronized {
    GDALStyleTable(underlying.GetStyleTable)
  }

  def setStyleTable(table: GDALStyleTable): Unit = AnyRef.synchronized {
    underlying.SetStyleTable(table.underlying)
  }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override def finalize(): Unit = {
    delete
    super.finalize()
  }
}

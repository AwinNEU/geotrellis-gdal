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

import org.gdal.gdal.MajorObject

import scala.collection.JavaConverters._
import java.util

trait GDALMajorObject {
  val underlying: MajorObject

  def setMetadata(metadata: Map[_, _], domain: String): Int = AnyRef.synchronized {
    val map = new util.Hashtable[Any, Any]()
    map.putAll(metadata.asJava)
    underlying.SetMetadata(map, domain)
  }

  def setMetadata(metadata: Map[_, _]): Int = AnyRef.synchronized {
    val map = new util.Hashtable[Any, Any]()
    map.putAll(metadata.asJava)
    underlying.SetMetadata(map)
  }

  def getDescription: Option[String] = AnyRef.synchronized {
    Option(underlying.GetDescription).filter(_.nonEmpty)
  }

  def setDescription(pszNewDesc: String): Unit = AnyRef.synchronized {
    underlying.SetDescription(pszNewDesc)
  }

  def getMetadataDomainList: Vector[String] = AnyRef.synchronized {
    underlying.GetMetadataDomainList.asScala.toVector.map(_.asInstanceOf[String])
  }

  def getMetadata_Dict(pszDomain: String): Map[String, String] = AnyRef.synchronized {
    underlying.GetMetadata_Dict(pszDomain).asInstanceOf[util.Map[String, String]].asScala.toMap
  }

  def getMetadata_Dict: Map[String, String] = AnyRef.synchronized {
    underlying.GetMetadata_Dict.asInstanceOf[util.Map[String, String]].asScala.toMap
  }

  def getMetadata_List(pszDomain: String): Vector[String] = AnyRef.synchronized {
    underlying.GetMetadata_List(pszDomain).asScala.toVector.map(_.asInstanceOf[String])
  }

  def getMetadata_List: Vector[String] = AnyRef.synchronized {
    underlying.GetMetadata_List().asScala.toVector.map(_.asInstanceOf[String])
  }

  def setMetadata(papszMetadata: Vector[_], pszDomain: String): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(papszMetadata.asJavaCollection)
    underlying.SetMetadata(vector, pszDomain)
  }

  def setMetadata(papszMetadata: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(papszMetadata.asJavaCollection)
    underlying.SetMetadata(vector)
  }

  def setMetadata(pszMetadataString: String, pszDomain: String): Int = AnyRef.synchronized {
    underlying.SetMetadata(pszMetadataString, pszDomain)
  }

  def setMetadata(pszMetadataString: String): Int = AnyRef.synchronized {
    underlying.SetMetadata(pszMetadataString)
  }

  def getMetadataItem(pszName: String, pszDomain: String): String = AnyRef.synchronized {
    underlying.GetMetadataItem(pszName, pszDomain)
  }

  def getMetadataItem(pszName: String): String = AnyRef.synchronized {
    underlying.GetMetadataItem(pszName)
  }

  def setMetadataItem(pszName: String, pszValue: String, pszDomain: String): Int = AnyRef.synchronized {
    underlying.SetMetadataItem(pszName, pszValue, pszDomain)
  }

  def setMetadataItem(pszName: String, pszValue: String): Int = AnyRef.synchronized {
    underlying.SetMetadataItem(pszName, pszValue)
  }
}

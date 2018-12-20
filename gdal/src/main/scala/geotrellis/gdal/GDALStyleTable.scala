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

import org.gdal.ogr.StyleTable

case class GDALStyleTable(underlying: StyleTable) {
  def addStyle(pszName: String, pszStyleString: String): Int = AnyRef.synchronized {
    underlying.AddStyle(pszName, pszStyleString)
  }

  def loadStyleTable(utf8_path: String): Int = AnyRef.synchronized {
    underlying.LoadStyleTable(utf8_path)
  }

  def saveStyleTable(utf8_path: String): Int = AnyRef.synchronized {
    underlying.SaveStyleTable(utf8_path)
  }

  def find(pszName: String): String = AnyRef.synchronized {
    underlying.Find(pszName)
  }

  def resetStyleStringReading: Unit = AnyRef.synchronized {
    underlying.ResetStyleStringReading
  }

  def getNextStyle: String = AnyRef.synchronized {
    underlying.GetNextStyle
  }

  def getLastStyleName: String = AnyRef.synchronized {
    underlying.GetLastStyleName
  }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override protected def finalize(): Unit = {
    delete
    super.finalize()
  }
}

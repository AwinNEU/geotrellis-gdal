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

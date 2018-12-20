package geotrellis.gdal

import org.gdal.gdal.RasterAttributeTable

case class GDALRasterAttributeTable(underlying: RasterAttributeTable) extends Cloneable {
  override def clone: GDALRasterAttributeTable = GDALRasterAttributeTable(underlying.Clone)

  def getColumnCount: Int = AnyRef.synchronized {
    underlying.GetColumnCount
  }

  def getNameOfCol(iCol: Int): String = AnyRef.synchronized {
    underlying.GetNameOfCol(iCol)
  }

  def getUsageOfCol(iCol: Int): Int = AnyRef.synchronized {
    underlying.GetUsageOfCol(iCol)
  }

  def getTypeOfCol(iCol: Int): Int = AnyRef.synchronized {
    underlying.GetTypeOfCol(iCol)
  }

  def getColOfUsage(eUsage: Int): Int = AnyRef.synchronized {
    underlying.GetColOfUsage(eUsage)
  }

  def getRowCount: Int = AnyRef.synchronized {
    underlying.GetRowCount
  }

  def getValueAsString(iRow: Int, iCol: Int): String = AnyRef.synchronized {
    underlying.GetValueAsString(iRow, iCol)
  }

  def getValueAsInt(iRow: Int, iCol: Int): Int = AnyRef.synchronized {
    underlying.GetValueAsInt(iRow, iCol)
  }

  def getValueAsDouble(iRow: Int, iCol: Int): Double = AnyRef.synchronized {
    underlying.GetValueAsDouble(iRow, iCol)
  }

  def setValueAsString(iRow: Int, iCol: Int, pszValue: String): Unit = AnyRef.synchronized {
    underlying.SetValueAsString(iRow, iCol, pszValue)
  }

  def setValueAsInt(iRow: Int, iCol: Int, nValue: Int): Unit = AnyRef.synchronized {
    underlying.SetValueAsInt(iRow, iCol, nValue)
  }

  def setValueAsDouble(iRow: Int, iCol: Int, dfValue: Double): Unit = AnyRef.synchronized {
    underlying.SetValueAsDouble(iRow, iCol, dfValue)
  }

  def setRowCount(nCount: Int): Unit = AnyRef.synchronized {
    underlying.SetRowCount(nCount)
  }

  def createColumn(pszName: String, eType: Int, eUsage: Int): Int = AnyRef.synchronized {
    underlying.CreateColumn(pszName, eType, eUsage)
  }

  def getLinearBinning(pdfRow0Min: Array[Double], pdfBinSize: Array[Double]): Boolean = AnyRef.synchronized {
    underlying.GetLinearBinning(pdfRow0Min, pdfBinSize)
  }

  def setLinearBinning(dfRow0Min: Double, dfBinSize: Double): Int = AnyRef.synchronized {
    underlying.SetLinearBinning(dfRow0Min, dfBinSize)
  }

  def getRowOfValue(dfValue: Double): Int = AnyRef.synchronized {
    underlying.GetRowOfValue(dfValue)
  }

  def changesAreWrittenToFile: Int = AnyRef.synchronized {
    underlying.ChangesAreWrittenToFile
  }

  def dumpReadable: Unit = AnyRef.synchronized {
    underlying.DumpReadable
  }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override protected def finalize(): Unit = {
    delete
    super.finalize()
  }
}

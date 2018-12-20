package geotrellis.gdal

import java.awt.Color
import java.awt.image.IndexColorModel

import org.gdal.gdal.{ColorTable, gdal}

case class GDALColorTable(underlying: ColorTable) extends Cloneable {
  override def clone: GDALColorTable = AnyRef.synchronized {
    GDALColorTable(underlying.Clone)
  }

  def getIndexColorModel(bits: Int): IndexColorModel = AnyRef.synchronized {
    underlying.getIndexColorModel(bits)
  }

  def getPaletteInterpretation: Int = AnyRef.synchronized {
    underlying.GetPaletteInterpretation
  }

  def getPaletteInterpretationName: String = AnyRef.synchronized {
    gdal.GetPaletteInterpretationName(getPaletteInterpretation)
  }

  def getCount: Int = AnyRef.synchronized {
    underlying.GetCount
  }

  def getColorEntry(entry: Int): Color = AnyRef.synchronized {
    underlying.GetColorEntry(entry)
  }

  def setColorEntry(entry: Int, centry: Color): Unit = AnyRef.synchronized {
    underlying.SetColorEntry(entry, centry)
  }

  def createColorRamp(nStartIndex: Int, startcolor: Color, nEndIndex: Int, endcolor: Color): Unit = AnyRef.synchronized {
    underlying.CreateColorRamp(nStartIndex, startcolor, nEndIndex, endcolor)
  }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override protected def finalize(): Unit = {
    delete
    super.finalize()
  }
}

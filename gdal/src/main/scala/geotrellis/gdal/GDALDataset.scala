package geotrellis.gdal

import org.gdal.gdal._
import org.gdal.ogr.{Feature, Geometry}
import org.gdal.osr.SpatialReference

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

case class GDALDataset(underlying: Dataset) extends GDALMajorObject {
  def buildOverviews(overviewlist: Array[Int], callback: ProgressCallback): Int = AnyRef.synchronized {
    underlying.BuildOverviews(null, overviewlist, callback)
  }

  def buildOverviews(overviewlist: Array[Int]): Int = AnyRef.synchronized {
    underlying.BuildOverviews(overviewlist)
  }

  def getGCPs: Vector[GCP] = AnyRef.synchronized {
    val gcps = new java.util.Vector[GCP]()
    underlying.GetGCPs(gcps)
    gcps.asScala.toVector
  }

  def getGeoTransform: Array[Double] = AnyRef.synchronized {
    underlying.GetGeoTransform()
  }

  def getLayer(index: Int): GDALLayer = AnyRef.synchronized {
    GDALLayer(underlying.GetLayer(index))
  }

  def getLayer(layerName: String): GDALLayer = AnyRef.synchronized {
    GDALLayer(underlying.GetLayer(layerName))
  }

  def getRasterXSize: Int = AnyRef.synchronized {
    underlying.getRasterXSize
  }

  def getRasterYSize: Int = AnyRef.synchronized {
    underlying.getRasterYSize
  }

  def getRasterCount: Int = AnyRef.synchronized {
    underlying.getRasterCount
  }

  def getDriver: GDALDriver = AnyRef.synchronized {
    GDALDriver(underlying.GetDriver)
  }

  def getRasterBand(nBand: Int): GDALBand = AnyRef.synchronized {
    GDALBand(underlying.GetRasterBand(nBand))
  }

  def getRasterBands(seq: Seq[Int] = 1 to getRasterCount): List[GDALBand] =
    seq.map(getRasterBand).toList

  def getProjection: Option[String] = AnyRef.synchronized {
    Option(underlying.GetProjection).filter(_.nonEmpty)
  }

  def getProjectionRef: Option[String] = AnyRef.synchronized {
    Option(underlying.GetProjectionRef).filter(_.nonEmpty)
  }

  def setProjection(prj: String): Int = AnyRef.synchronized {
    underlying.SetProjection(prj)
  }

  def setGeoTransform(argin: Array[Double]): Int = AnyRef.synchronized {
    underlying.SetGeoTransform(argin)
  }

  def buildOverviews(resampling: String, overviewlist: Array[Int], callback: ProgressCallback): Int = AnyRef.synchronized {
    underlying.BuildOverviews(resampling, overviewlist, callback)
  }

  def buildOverviews(resampling: String, overviewlist: Array[Int]): Int = AnyRef.synchronized {
    underlying.BuildOverviews(resampling, overviewlist)
  }

  def getGCPCount: Int = AnyRef.synchronized {
    underlying.GetGCPCount
  }

  def getGCPProjection: String = AnyRef.synchronized {
    underlying.GetGCPProjection
  }

  def setGCPs(nGCPs: Array[GCP], pszGCPProjection: String): Int = AnyRef.synchronized {
    underlying.SetGCPs(nGCPs, pszGCPProjection)
  }

  def flushCache(): Unit = AnyRef.synchronized {
    underlying.FlushCache()
  }

  def addBand(datatype: Int, options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.AddBand(datatype, vector)
  }

  def addBand(datatype: Int): Int = AnyRef.synchronized {
    underlying.AddBand(datatype)
  }

  def addBand: Int = AnyRef.synchronized {
    underlying.AddBand
  }

  def createMaskBand(nFlags: Int): Int = AnyRef.synchronized {
    underlying.CreateMaskBand(nFlags)
  }

  def getFileList: Vector[String] = AnyRef.synchronized {
    val list = underlying.GetFileList()
    if(list == null) Vector()
    else list.asScala.toVector.map(_.asInstanceOf[String])
  }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: SWIGTYPE_p_int, buf_ysize: SWIGTYPE_p_int, buf_type: SWIGTYPE_p_int, band_list: Array[Int], options: Vector[_]): Int = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    underlying.AdviseRead(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, band_list, vector)
  }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: SWIGTYPE_p_int, buf_ysize: SWIGTYPE_p_int, buf_type: SWIGTYPE_p_int, band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.AdviseRead(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, band_list)
    }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: SWIGTYPE_p_int, buf_ysize: SWIGTYPE_p_int, buf_type: SWIGTYPE_p_int): Int =
    AnyRef.synchronized {
      underlying.AdviseRead(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type)
    }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: SWIGTYPE_p_int, buf_ysize: SWIGTYPE_p_int): Int =
    AnyRef.synchronized {
      underlying.AdviseRead(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize)
    }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: SWIGTYPE_p_int): Int =
    AnyRef.synchronized {
      underlying.AdviseRead(xoff, yoff, xsize, ysize, buf_xsize)
    }

  def adviseRead(xoff: Int, yoff: Int, xsize: Int, ysize: Int): Int =
    AnyRef.synchronized {
      underlying.AdviseRead(xoff, yoff, xsize, ysize)
    }

  def createLayer(name: String, srs: SpatialReference, geom_type: Int, options: Vector[_]): GDALLayer = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALLayer(underlying.CreateLayer(name, srs, geom_type, vector))
  }

  def createLayer(name: String, srs: SpatialReference, geom_type: Int): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.CreateLayer(name, srs, geom_type))
    }

  def createLayer(name: String, srs: SpatialReference): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.CreateLayer(name, srs))
    }

  def createLayer(name: String): GDALLayer = AnyRef.synchronized {
    GDALLayer(underlying.CreateLayer(name))
  }

  def copyLayer(src_layer: GDALLayer, new_name: String, options: Vector[_]): GDALLayer = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALLayer(underlying.CopyLayer(src_layer.underlying, new_name, vector))
  }

  def copyLayer(src_layer: GDALLayer, new_name: String): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.CopyLayer(src_layer.underlying, new_name))
    }

  def deleteLayer(index: Int): Int = AnyRef.synchronized {
    underlying.DeleteLayer(index)
  }

  def getLayerCount: Int =
    AnyRef.synchronized {
      underlying.GetLayerCount
    }

  def getLayerByIndex(index: Int): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.GetLayerByIndex(index))
    }

  def getLayerByName(layer_name: String): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.GetLayerByName(layer_name))
    }

  def resetReading(): Unit =
    AnyRef.synchronized {
      underlying.ResetReading
    }

  def getNextFeature: Feature =
    AnyRef.synchronized {
      underlying.GetNextFeature
    }

  def testCapability(cap: String): Boolean =
    AnyRef.synchronized {
      underlying.TestCapability(cap)
    }

  def executeSQL(statement: String, spatialFilter: Geometry, dialect: String): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.ExecuteSQL(statement, spatialFilter, dialect))
    }

  def executeSQL(statement: String, spatialFilter: Geometry): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.ExecuteSQL(statement, spatialFilter))
    }

  def executeSQL(statement: String): GDALLayer =
    AnyRef.synchronized {
      GDALLayer(underlying.ExecuteSQL(statement))
    }

  def releaseResultSet(layer: GDALLayer): Unit = AnyRef.synchronized {
    underlying.ReleaseResultSet(layer.underlying)
  }

  def getStyleTable: GDALStyleTable = AnyRef.synchronized {
    GDALStyleTable(underlying.GetStyleTable)
  }

  def setStyleTable(table: GDALStyleTable): Unit = AnyRef.synchronized {
    underlying.SetStyleTable(table.underlying)
  }

  def startTransaction(force: Int): Int = AnyRef.synchronized {
    underlying.StartTransaction(force)
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

  def readRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace)
    }

  def readRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Byte], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Byte], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Byte], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Byte], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Short], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Short], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Short], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Short], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Int], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Int], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Int], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Int], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Float], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Float], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Float], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Float], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Double], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Double], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace, nLineSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Double], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
    }

  def readRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayOut: Array[Double], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize,buf_ysize,buf_type, regularArrayOut, band_list)
    }

  def writeRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list, nPixelSpace)
    }

  def writeRaster_Direct(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, nioBuffer: ByteBuffer, band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster_Direct(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, nioBuffer, band_list)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Byte], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Byte], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Byte], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Byte], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Short], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Short], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Short], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Short], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Int], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Int], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Int], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Int], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Float], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Float], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Float], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Float], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Double], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int, nBandSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace, nBandSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Double], band_list: Array[Int], nPixelSpace: Int, nLineSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace, nLineSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Double], band_list: Array[Int], nPixelSpace: Int): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list, nPixelSpace)
    }

  def writeRaster(xoff: Int, yoff: Int, xsize: Int, ysize: Int, buf_xsize: Int, buf_ysize: Int, buf_type: Int, regularArrayIn: Array[Double], band_list: Array[Int]): Int =
    AnyRef.synchronized {
      underlying.WriteRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayIn, band_list)
    }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override def finalize(): Unit = {
    delete
    super.finalize()
  }
}

package geotrellis.gdal

import org.gdal.gdal.{Driver, ProgressCallback}

import scala.collection.JavaConverters._

case class GDALDriver(underlying: Driver) extends GDALMajorObject {
  def create(name: String, xsize: Int, ysize: Int, bands: Int, eType: Int, options: Array[String]): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.Create(name, xsize, ysize, bands, eType, options))
  }

  def create(name: String, xsize: Int, ysize: Int, bands: Int, options: Array[String]): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.Create(name, xsize, ysize, bands, options))
  }

  def createCopy(name: String, src: GDALDataset, strict: Int, options: Array[String]): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.CreateCopy(name, src.underlying, strict, options))
  }

  def createCopy(name: String, src: GDALDataset, options: Vector[_]): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALDataset(underlying.CreateCopy(name, src.underlying, vector))
  }

  def createCopy(name: String, src: GDALDataset, options: Array[String]): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.CreateCopy(name, src.underlying, options))
  }

  def getShortName: String = AnyRef.synchronized {
    underlying.getShortName
  }

  def getLongName: String = AnyRef.synchronized {
    underlying.getLongName
  }

  def getHelpTopic: String = AnyRef.synchronized {
    underlying.getHelpTopic
  }

  def create(utf8_path: String, xsize: Int, ysize: Int, bands: Int, eType: Int, options: Vector[_]): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALDataset(underlying.Create(utf8_path, xsize, ysize, bands, eType, vector))
  }

  def create(utf8_path: String, xsize: Int, ysize: Int, bands: Int, eType: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.Create(utf8_path, xsize, ysize, bands, eType))
  }

  def create(utf8_path: String, xsize: Int, ysize: Int, bands: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.Create(utf8_path, xsize, ysize, bands))
  }

  def create(utf8_path: String, xsize: Int, ysize: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.Create(utf8_path, xsize, ysize))
  }

  def createCopy(utf8_path: String, src: GDALDataset, strict: Int, options: Vector[_], callback: ProgressCallback): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALDataset(underlying.CreateCopy(utf8_path, src.underlying, strict, vector, callback))
  }

  def createCopy(utf8_path: String, src: GDALDataset, strict: Int, options: Vector[_]): GDALDataset = AnyRef.synchronized {
    val vector = new java.util.Vector[Any]()
    vector.addAll(options.asJavaCollection)
    GDALDataset(underlying.CreateCopy(utf8_path, src.underlying, strict, vector))
  }

  def createCopy(utf8_path: String, src: GDALDataset, strict: Int): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.CreateCopy(utf8_path, src.underlying, strict))
  }

  def createCopy(utf8_path: String, src: GDALDataset): GDALDataset = AnyRef.synchronized {
    GDALDataset(underlying.CreateCopy(utf8_path, src.underlying))
  }

  def delete(utf8_path: String): Int = AnyRef.synchronized {
    underlying.Delete(utf8_path)
  }

  def rename(newName: String, oldName: String): Int = AnyRef.synchronized {
    underlying.Rename(newName, oldName)
  }

  def copyFiles(newName: String, oldName: String): Int = AnyRef.synchronized {
    underlying.CopyFiles(newName, oldName)
  }

  def register: Int = AnyRef.synchronized {
    underlying.Register
  }

  def deregister(): Unit = AnyRef.synchronized {
    underlying.Deregister
  }

  def delete: Unit = AnyRef.synchronized {
    underlying.delete
  }

  override def finalize(): Unit = {
    delete
    super.finalize()
  }
}

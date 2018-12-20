package geotrellis.gdal

import org.scalatest._
import org.scalatest.BeforeAndAfterAll
import org.gdal.gdal.{Driver, gdal}

trait OnlyIfGdalInstalled extends FunSpec with BeforeAndAfterAll {

  def ifGdalInstalled(f: => Unit): Unit = {
    try {
      gdal.AllRegister()
      f
    } catch {
      case e: java.lang.UnsatisfiedLinkError =>
        info(e.toString)
        ignore("Java GDAL bindings not installed, skipping"){}
    }
  }

  val jpeg2000Drivers: List[String] =
    List("JPEG2000", "JP2ECW", "JP2KAK", "JP2Lura", "JP2MrSID", "JP2OpenJPEG", "JPIPKAK")

  // Only run f if some GDAL JPEG2000 plugin is installed.
  def ifGdalWithJpeg2000Installed(f: => Unit): Unit = {
    ifGdalInstalled {
      val driversInstalled: List[Driver] = jpeg2000Drivers map { gdal.GetDriverByName }

      if (driversInstalled.exists(_ != null)) f
      else ignore("GDAL JPEG2000 plugins not installed, skipping") { }
    }
  }
}

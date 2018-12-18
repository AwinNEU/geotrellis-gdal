package geotrellis.gdal

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.testkit._
import geotrellis.raster.io.geotiff._

import org.scalatest._

class GdalReaderSpec extends FunSpec
  with RasterMatchers
  with OnlyIfGdalInstalled
{

  val path = "src/test/resources/data/slope.tif"

  describe("reading a GeoTiff") {
    ifGdalInstalled {
      val reader = GdalReader(path)
      it("should match one read with GeoTools") {
        println("Reading with GDAL...")
        val raster = reader.read()
        val gdRaster = raster.tile.band(0)
        val gdExt = raster.extent
        println("Reading with GeoTools....")
        val Raster(gtRaster, gtExt) = SinglebandGeoTiff(path).raster
        println("Done.")

        gdExt.xmin should be(gtExt.xmin +- 0.00001)
        gdExt.xmax should be(gtExt.xmax +- 0.00001)
        gdExt.ymin should be(gtExt.ymin +- 0.00001)
        gdExt.ymax should be(gtExt.ymax +- 0.00001)

        gdRaster.cols should be(gtRaster.cols)
        gdRaster.rows should be(gtRaster.rows)

        gdRaster.cellType.toString.take(7) should be(gtRaster.cellType.toString.take(7))

        println("Comparing rasters...")
        for (col <- 0 until gdRaster.cols) {
          for (row <- 0 until gdRaster.rows) {
            val actual = gdRaster.getDouble(col, row)
            val expected = gtRaster.getDouble(col, row)
            withClue(s"At ($col, $row): GDAL - $actual  GeoTools - $expected") {
              isNoData(actual) should be(isNoData(expected))
              if (isData(actual)) actual should be(expected)
            }
          }
        }
      }

      it("should do window reads") {
        val gtiff = MultibandGeoTiff(path)
        val gridBounds = reader.rasterDataSet.gridBounds.split(15, 21)

        gridBounds.map { case gb =>
          val actualTile = reader.read(gb).tile
          val expectedTile = gtiff.tile.crop(gb)

          assertEqual(actualTile, expectedTile)
        }
      }

      it("should read CRS from file") {
        val rasterDataSet = Gdal.open("src/test/resources/data/geotiff-test-files/all-ones.tif")
        rasterDataSet.crs should equal(Some(LatLng))
      }
    }
  }

  describe("reading a JPEG2000") {
    ifGdalWithJpeg2000Installed {
      val lengthExpected = 100
      type TypeExpected = UShortCells
      val jpeg2000Path = "src/test/resources/data/jpeg2000-test-files/testJpeg2000.jp2"
      val jpegTiffPath = "src/test/resources/data/jpeg2000-test-files/jpegTiff.tif"

      val dataset = Gdal.open(jpeg2000Path)
      val jpegReader = GdalReader(dataset)
      val tiffReader = GdalReader(jpegTiffPath)

      val gridBounds: Iterator[GridBounds] =
        dataset.gridBounds.split(20, 15)

      it("should read a JPEG2000 from a file") {
        val raster = jpegReader.read()
        val tile = raster.tile
        val extent = raster.rasterExtent

        extent.cols should be (lengthExpected)
        extent.rows should be (lengthExpected)
        tile.cellType shouldBe a [TypeExpected]
      }

      it("should do window reads") {
        gridBounds.map { gb =>
          val actualTile = jpegReader.read(gb)
          val expectedTile = tiffReader.read(gb)

          assertEqual(actualTile, expectedTile)
        }
      }
    }
  }
}

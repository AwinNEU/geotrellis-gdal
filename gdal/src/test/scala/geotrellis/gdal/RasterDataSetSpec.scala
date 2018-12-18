package geotrellis.gdal

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.testkit._

import org.scalatest._


class RasterDataSetSpec extends FunSpec
  with RasterMatchers
  with OnlyIfGdalInstalled
{

  val path = "src/test/resources/data/slope.tif"
  val dataset = Gdal.open(path)

  describe("Resampling a RasterDataSet") {
    ifGdalInstalled {
      it("should resample to a target CellSize") {
        val targetSize = CellSize(20.0, 20.0)

        val resampled = dataset.resample(targetSize, NearestNeighbor)
        val reader = GdalReader(resampled)

        val actualTile = reader.read()
        val expectedTile = GdalReader("src/test/resources/data/slope-cellsize-resample.tif").read()

        assertEqual(actualTile, expectedTile)
        resampled.close()
      }

      it("should resample to a target cols and rows") {
        val targetDimensions = (1000, 1500)
        val resampled = dataset.resample(targetDimensions, NearestNeighbor)
        val reader = GdalReader(resampled)

        val actualTile = reader.read()
        val expectedTile = GdalReader("src/test/resources/data/slope-dimensions-resample.tif").read()

        assertEqual(actualTile, expectedTile)
        resampled.close()
      }
    }
  }
}

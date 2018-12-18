package geotrellis.gdal

import geotrellis.gdal.io.hadoop._
import geotrellis.gdal.io.hadoop.GdalInputFormat.{GdalFileInfo, GdalRasterInfo}
import geotrellis.proj4.LatLng
import geotrellis.raster.{Tile, UShortCells}
import geotrellis.spark._
import geotrellis.spark.ingest._
import geotrellis.spark.testkit.TestEnvironment
import geotrellis.spark.tiling._

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import java.time.ZonedDateTime

import org.scalatest._

class IngestSpec extends FunSpec
    with Matchers
    with OnlyIfGdalInstalled
    with TestEnvironment
{

  describe("Ingest") {
    ifGdalInstalled {
      val expectedKeys = List(
        SpaceTimeKey(SpatialKey(1,1),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,0),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,1),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,0),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,1),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,1),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,1),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,1),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,1),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(1,0),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(1,0),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,0),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(1,0),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(1,1),TemporalKey(ZonedDateTime.parse("2006-01-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(1,1),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,0),TemporalKey(ZonedDateTime.parse("2006-02-15T00:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(2,0),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z"))),
        SpaceTimeKey(SpatialKey(0,0),TemporalKey(ZonedDateTime.parse("2006-03-16T12:00:00.000Z")))
      )

      val resourcesPath = "src/test/resources"

      it("should ingest time-band NetCDF") {
        val source = sc.netCdfRDD(new Path(resourcesPath, "ipcc-access1-tasmin.nc"))
        Ingest[TemporalProjectedExtent, SpaceTimeKey](source, LatLng, FloatingLayoutScheme(256)) { (rdd, level) =>
          val ingestKeys = rdd.keys.collect()
          info(ingestKeys.toList.toString)
          // Ingest uses a tileSize of 512x512, so we have less tiles
          ingestKeys should contain theSameElementsAs expectedKeys
        }
      }

      it("should ingest time-band NetCDF in stages") {
        val source = sc.netCdfRDD(new Path(resourcesPath, "ipcc-access1-tasmin.nc"))
        val (_, rmd) = source.collectMetadata[SpaceTimeKey](LatLng, FloatingLayoutScheme(256))
        val tiled = source.cutTiles[SpaceTimeKey](rmd)
        val ingestKeys = tiled.keys.collect()
        ingestKeys should contain theSameElementsAs expectedKeys
      }

      ifGdalWithJpeg2000Installed {
        val lengthExpected = 100
        type TypeExpected = UShortCells
        val jpeg2000Path = "src/test/resources/data/jpeg2000-test-files/testJpeg2000.jp2"

        it("should load a JPEG2000 into an RDD") {
          val tileRdd: RDD[(GdalRasterInfo, Tile)] =
            sc.gdalRDD(new org.apache.hadoop.fs.Path(jpeg2000Path))

          val first = tileRdd.first()
          val fileInfo: GdalFileInfo = first._1.file
          val tile: Tile = first._2

          fileInfo.rasterExtent.cols should be(lengthExpected)
          fileInfo.rasterExtent.rows should be(lengthExpected)
          tile.cellType shouldBe a[TypeExpected]
        }
      }
    }
  }
}

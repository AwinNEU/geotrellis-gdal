package geotrellis.gdal

import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{CellSize, GridExtent}
import geotrellis.raster.io.geotiff.AutoHigherResolution
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.reproject.Reproject.{Options => ReprojectOptions}
import geotrellis.vector.Extent
import geotrellis.raster.testkit._

import org.gdal.gdal.Dataset
import cats.implicits._
import cats.effect.{ContextShift, IO}
import org.scalatest._

import java.io.File
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

class DatasetSpec extends FunSpec with RasterMatchers with OnlyIfGdalInstalled {
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/data/aspect-tiled.tif"
  def filePathByIndex(i: Int): String = s"${new File("").getAbsolutePath()}/src/test/resources/data/aspect-tiled-$i.tif"

  /** Simulate possible RF backsplash calls */
  def dirtyCallsDS(ds: Dataset): Dataset = {
    val Extent(xmin, ymin, xmax, ymax) = ds.rasterExtent.extent
    val string = ds.crs.map(_.proj4jCrs)
    val CellSize(ch, cw) = ds.cellSize
    val Extent(xmin1, ymin1, xmax1, ymax1) = ds.extent

    ds
  }

  val reprojectOptions =
    GDALWarpOptions(
      Some("VRT"),
      Some(NearestNeighbor),
      None,
      Some(CellSize(19.1, 19.1)),
      true,
      None,
      Some(CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs ")),
      Some(WebMercator),
      None,
      None,
      List("-9999.0"),
      Nil,
      Some(AutoHigherResolution),
      Nil, false, None, false, false, false, None, Nil, None, None, false, false, false, None, false,
      false, Nil, None, None, None, None, None, false, false, false, None, false, Nil, Nil, Nil, None
    )

  val resampleOptions =
    GDALWarpOptions(
      Some("VRT"),
      Some(NearestNeighbor),
      None,
      Some(CellSize(19.1, 19.1)),
      true,
      None,
      None,
      None,
      None,
      None,
      List("-9999.0"),
      Nil,
      Some(AutoHigherResolution),
      Nil, false, None, false, false, false, None, Nil, None, None, false, false, false, None, false,
      false, Nil, None, None, None, None, None, false, false, false, None, false, Nil, Nil, Nil, None
    )

  def dsreproject(dataset: Dataset): Dataset = GDAL.warp("", dataset, reprojectOptions, None)
  def dsresample(dataset: Dataset, uri: Option[String]): Dataset = GDAL.warp("", dataset, resampleOptions, uri.map(str => str -> List(reprojectOptions)))

  def dsreprojectOpt(dataset: Dataset): Dataset =
    GDAL.warp(
      "",
      dataset,
      GDALWarpOptions().reproject(
        rasterExtent = GridExtent(Extent(630000.0, 215000.0, 645000.0, 228500.0),10.0,10.0).toRasterExtent,
        CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs ") ,
        WebMercator,
        ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(19.1, 19.1).some)
      ),
      None
    )
  def dsresampleOpt(dataset: Dataset): Dataset =
    GDAL.warp(
      "",
      dataset,
      GDALWarpOptions().reproject(
        rasterExtent = GridExtent(Extent(630000.0, 215000.0, 645000.0, 228500.0), 10.0, 10.0).toRasterExtent,
        CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs "),
        WebMercator,
        ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(19.1, 19.1).some)
      )
      .resample(
        GridExtent(Extent(-8769161.632988561, 4257695.349540888, -8750625.653629405, 4274473.277249484), 19.1, 19.1).toRasterExtent,
        TargetRegion(GridExtent(Extent(-8769161.632988561, 4257695.349540888, -8750625.653629405, 4274473.277249484), 19.1, 19.1).toRasterExtent)
      ),
      None
    )

  def parellSpec(n: Int = 1000)(implicit cs: ContextShift[IO]): List[(Dataset, Dataset, Dataset)] = {
    println(java.lang.Thread.activeCount())

    // to make it work with weak refs we have to remember all the datasets
    val res = (1 to n).toList.flatMap { _ =>
      (0 to 4).flatMap { i =>
        val path = filePathByIndex(i)
        List(IO {
          val fst = dirtyCallsDS(GDAL.open(path))
          val dst = dirtyCallsDS(dsreproject(fst))
          val trd = dirtyCallsDS(dsresample(dst, Some(path)))

          (fst, dst, trd)
        }, IO {
          val fst = dirtyCallsDS(GDAL.open(path))
          val dst = dirtyCallsDS(dsreproject(fst))
          val trd = dirtyCallsDS(dsresample(dst, Some(path)))

          (fst, dst, trd)
        }, IO {
          val fst = dirtyCallsDS(GDAL.open(path))
          val dst = dirtyCallsDS(dsreproject(fst))
          val trd = dirtyCallsDS(dsresample(dst, Some(path)))

          (fst, dst, trd)
        })
      }
    }.parSequence.unsafeRunSync

    println(java.lang.Thread.activeCount())

    res
  }

  describe("GDALDatasetSpec") {
    ifGdalInstalled {
      it("GDAL.open should work") {
        val path = filePathByIndex(1)
        val fst = dirtyCallsDS(GDAL.open(path))
        val dst = dirtyCallsDS(dsreproject(fst))
        val trd = dirtyCallsDS(dsresample(dst, Some(path)))
      }

      it("GDAL.open multithreaded test forkjoin pool") {
        val i = 1000
        implicit val cs = IO.contextShift(ExecutionContext.global)

        val res = parellSpec(i)
      }

      it("GDAL.open multithreaded test fixed thread pool") {
        val i = 1000
        val n = 200
        val pool = Executors.newFixedThreadPool(n)
        val ec = ExecutionContext.fromExecutor(pool)
        implicit val cs = IO.contextShift(ec)

        val res = parellSpec(i)
      }
    }
  }

  describe("Dataset fromGDALWarpOptionsH build test") {
    it("should build and keep history, nothing stored in the Dataset pool case") {
      val vrtPlan = List(GDALWarpOptions(), reprojectOptions, resampleOptions)
      val (result, history) = GDAL.fromGDALWarpOptionsH(filePath, vrtPlan)

      history shouldNot contain (result)
      history.length shouldBe vrtPlan.length
      GDAL.cacheCleanUp
    }

    it("should build and keep history, only initial Dataset is stored in the Dataset pool case") {
      val baseDataset = GDAL.open(filePath)
      val vrtPlan = List(GDALWarpOptions(), reprojectOptions, resampleOptions)

      val (result, history) = GDAL.fromGDALWarpOptionsH(filePath, vrtPlan, baseDataset)

      history shouldNot contain (result)
      history.length shouldBe vrtPlan.length
      GDAL.cacheCleanUp
    }

    it("should build and keep history, two Datasets are stored in the Dataset pool case") {
      val baseDataset = GDAL.open(filePath)
      val firstVRT = GDAL.warp("", baseDataset, GDALWarpOptions(), Some(filePath, Nil))
      val vrtPlan = List(GDALWarpOptions(), reprojectOptions, resampleOptions)

      val (result, history) = GDAL.fromGDALWarpOptionsH(filePath, vrtPlan, baseDataset)

      history shouldNot contain (result)
      history should contain (firstVRT)
      history.length shouldBe vrtPlan.length
      GDAL.cacheCleanUp
    }

    it("should build and keep history, three Datasets are stored in the Dataset pool case") {
      val baseDataset = GDAL.open(filePath)
      val firstVRT = GDAL.warp("", baseDataset, GDALWarpOptions(), Some(filePath, Nil))
      val secondVRT = GDAL.warp("", firstVRT, reprojectOptions, Some(filePath, List(GDALWarpOptions())))

      val vrtPlan = List(GDALWarpOptions(), reprojectOptions, resampleOptions)

      val (result, history) = GDAL.fromGDALWarpOptionsH(filePath, vrtPlan, baseDataset)

      history shouldNot contain (result)
      history should contain (firstVRT)
      history should contain (secondVRT)
      history.length shouldBe vrtPlan.length
      GDAL.cacheCleanUp
    }

    it("should build and keep history, four Datasets are stored in the Dataset pool case") {
      val baseDataset = GDAL.open(filePath)
      val firstVRT = GDAL.warp("", baseDataset, GDALWarpOptions(), Some(filePath, Nil))
      val secondVRT = GDAL.warp("", firstVRT, reprojectOptions, Some(filePath, List(GDALWarpOptions())))
      val thirdVRT = GDAL.warp("", secondVRT, resampleOptions, Some(filePath, List(GDALWarpOptions(), reprojectOptions)))
      val vrtPlan = List(GDALWarpOptions(), reprojectOptions, resampleOptions, resampleOptions)

      val (result, history) = GDAL.fromGDALWarpOptionsH(filePath, vrtPlan, baseDataset)

      history shouldNot contain (result)
      history should contain (firstVRT)
      history should contain (secondVRT)
      history should contain (thirdVRT)
      history.length shouldBe vrtPlan.length
      GDAL.cacheCleanUp
    }
  }

  describe("GDALWarp transformations") {
    it("optimized transformation should behave in a same way as a list of warp applications") {
      val base = GDAL.open(filePath)

      val originalReproject = dsreproject(base)
      val originalResample = dsresample(originalReproject, Some(filePath))

      val optimizedReproject = dsreprojectOpt(base)
      val optimizedResample = dsresampleOpt(base)

      originalReproject.rasterExtent shouldBe optimizedReproject.rasterExtent
      originalResample.rasterExtent shouldBe optimizedResample.rasterExtent
    }
  }
}

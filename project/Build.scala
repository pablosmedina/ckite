import sbt.Keys._
import sbt._
import sbt.Defaults.itSettings
import spray.revolver.RevolverPlugin._


object CKite extends Build {
  import Dependencies._
  import Settings._

  lazy val root: Project = Project("root", file("."))
     .settings(basicSettings: _*)
     .settings(formatSettings: _*)
     .settings(libraryDependencies ++= 
          compile(slf4j, scrooge, thrift, finagleCore, finagleThrift, config,  mapdb, kryo) ++
          test(scalaTest, logback, finagleHttp, jacksonAfterBurner, jacksonScala) )
}
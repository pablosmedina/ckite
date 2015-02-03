import sbt.Keys._
import sbt._
import sbt.Defaults.itSettings
import spray.revolver.RevolverPlugin._

object CKite extends Build {

  import Dependencies._
  import Settings._

  lazy val root: Project = Project("root", file("."))
     .settings(basicSettings: _*)
     .settings(sonatypeSettings: _*)
     .settings(formatSettings: _*)
     .settings(libraryDependencies ++= 
          compile(slf4j, config, kryo) ++
          test(scalaTest, logback))

  lazy val ckiteFinagle: Project = Project("ckite-finagle", file("ckite-finagle"))
    .dependsOn(root)
    .settings(scroogeSettings: _*)
    .settings(formatSettings: _*)
    .settings(libraryDependencies ++=
    compile(slf4j, scrooge, thrift, finagleCore, finagleThrift) ++
      test(scalaTest, logback, finagleHttp, jacksonAfterBurner, jacksonScala) )

  lazy val ckiteMapDB: Project = Project("ckite-mapdb", file("ckite-mapdb"))
    .dependsOn(root)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(libraryDependencies ++=
    compile(mapdb) ++
      test(scalaTest, logback) )

}

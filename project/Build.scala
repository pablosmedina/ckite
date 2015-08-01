import sbt.Keys._
import sbt._
import sbt.Defaults.itSettings
import spray.revolver.RevolverPlugin._

object CKite extends Build {

  import Dependencies._
  import Settings._

  lazy val ckite: Project = Project("ckite", file("."))
    .aggregate(ckiteCore,ckiteFinagle,ckiteMapDB)
     .settings(basicSettings: _*)
     .settings(sonatypeSettings: _*)
     .settings(formatSettings: _*)

    lazy val ckiteCore: Project = Project("ckite-core", file("ckite-core"))
     .settings(basicSettings: _*)
     .settings(sonatypeSettings: _*)
     .settings(formatSettings: _*)
     .settings(libraryDependencies ++= 
          compile(slf4j, config, kryo) ++
          test(scalaTest, logback))   


  lazy val ckiteFinagle: Project = Project("ckite-finagle", file("ckite-finagle"))
    .dependsOn(ckiteCore)
    .settings(basicSettings: _*)
    .settings(scroogeSettings: _*)
    .settings(sonatypeSettings: _*)
    .settings(formatSettings: _*)
    .settings(libraryDependencies ++=
    compile(slf4j, scrooge, thrift, finagleCore, finagleThrift) ++
      test(scalaTest, logback, finagleHttp, jacksonAfterBurner, jacksonScala) )

  lazy val ckiteMapDB: Project = Project("ckite-mapdb", file("ckite-mapdb"))
    .dependsOn(ckiteCore)
    .settings(basicSettings: _*)
    .settings(sonatypeSettings: _*)
    .settings(formatSettings: _*)
    .settings(libraryDependencies ++=
    compile(mapdb) ++
      test(scalaTest, logback) )

}

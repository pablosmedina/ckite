import sbt._

object Dependencies {

  val finagleV   = "6.27.0"
  val jacksonV   = "2.4.4"

  val slf4j               =       "org.slf4j"                       %   "slf4j-api"                    % "1.7.7"
  val scrooge             =       "com.twitter"                     %%  "scrooge-core"                 % "3.20.0" exclude("org.scala-lang", "scala-library")
  val thrift              =       "org.apache.thrift"               %   "libthrift"                    % "0.9.1"  exclude("org.apache.httpcomponents", "httpclient") exclude("org.apache.httpcomponents", "httpcore") exclude("org.slf4j", "slf4j-api") exclude("org.apache.commons", "commons-lang3")
  val finagleCore         =       "com.twitter"                     %%  "finagle-core"                 % finagleV exclude("com.twitter", "util-logging_2.11") exclude("com.twitter", "util-app_2.11")
  val finagleThrift       =       "com.twitter"                     %%  "finagle-thrift"               % finagleV exclude("org.scala-lang", "scala-library") exclude("org.apache.thrift", "libthrift")
  val finagleHttp         =       "com.twitter"                     %%  "finagle-http"                 % finagleV
  val config              =       "com.typesafe"                    %   "config"                       % "1.0.2"
  val mapdb               =       "org.mapdb"                       %   "mapdb"                        % "0.9.13"
  val kryo                =       "com.esotericsoftware.kryo"       %   "kryo"                         % "2.22"
  val jacksonAfterBurner  =       "com.fasterxml.jackson.module"    %   "jackson-module-afterburner"   % jacksonV
  val jacksonScala        =       "com.fasterxml.jackson.module"    %%  "jackson-module-scala"         % jacksonV
  val scalaTest           =       "org.scalatest"                   %%  "scalatest"                    % "2.2.2"
  val logback             =       "ch.qos.logback"                  %   "logback-classic"              % "1.1.2"

  def compile(deps: ModuleID*): Seq[ModuleID]   = deps map (_ % "compile")
  def provided(deps: ModuleID*): Seq[ModuleID]  = deps map (_ % "provided")
  def test(deps: ModuleID*): Seq[ModuleID]      = deps map (_ % "test")
  def runtime(deps: ModuleID*): Seq[ModuleID]   = deps map (_ % "runtime")
  def it(deps: ModuleID*): Seq[ModuleID]        = deps map (_ % "it")
  
}

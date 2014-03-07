name := "ckite"

organization := "io.ckite"

version := "0.1.2-SNAPSHOT"

scalaVersion := "2.10.2"

publishMavenStyle := true

publishArtifact in Test := false

crossPaths := false

pomIncludeRepository := { x => false }

resolvers += "twitter-repo" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
	"org.slf4j" % "slf4j-api" % "1.6.4",
	"com.twitter" %% "scrooge-core" % "3.9.0" exclude("org.scala-lang", "scala-library"),
	"org.apache.thrift" % "libthrift" % "0.9.1" exclude("org.apache.httpcomponents", "httpclient") exclude("org.apache.httpcomponents", "httpcore") exclude("org.slf4j", "slf4j-api") exclude("org.apache.commons", "commons-lang3"),
  "com.twitter" %% "finagle-core" % "6.6.2" exclude("com.twitter", "util-logging_2.10") exclude("com.twitter", "util-app_2.10"),
	"com.twitter" %% "finagle-thrift" % "6.6.2" exclude("org.scala-lang", "scala-library") exclude("org.apache.thrift", "libthrift"),
	"com.twitter" %% "finagle-http" % "6.6.2" exclude("org.scala-lang", "scala-library"),
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.1.3" exclude("org.scala-lang", "scala-library") exclude("com.google.guava", "guava"),
	"com.typesafe" % "config" % "1.0.2",
	"org.mapdb" % "mapdb" % "0.9.8",
	"org.scalatest" % "scalatest_2.10" % "2.0.M6" % "test",
  "ch.qos.logback" % "logback-classic" % "1.0.0" % "test",
	"junit" % "junit" % "4.8.1" % "test"
)

EclipseKeys.withSource := true

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources",
   	base / "src/main/thrift"
  )
}

com.twitter.scrooge.ScroogeSBT.newSettings

scroogeThriftOutputFolder in Compile  := file("src/main/scala")

publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) 
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else                             
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := {
  <url>http://ckite.io</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/pablosmedina/ckite.git</connection>
    <developerConnection>scm:git:git@github.com:pablosmedina/ckite.git</developerConnection>
    <url>github.com/pablosmedina/ckite.git</url>
  </scm>
  <developers>
    <developer>
      <id>pmedina</id>
      <name>Pablo S. Medina</name>
      <url>https://twitter.com/pablosmedina</url>
    </developer>
  </developers>
}


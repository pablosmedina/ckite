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
	"ch.qos.logback" % "logback-classic" % "1.0.0",
	"com.twitter" %% "scrooge-core" % "3.9.0" % "compile",
	"org.apache.thrift" % "libthrift" % "0.9.1",
	"com.twitter" %% "finagle-thrift" % "6.6.2",
	"com.twitter" %% "finagle-http" % "6.6.2",
	"com.twitter" %% "twitter-server" % "1.0.2",
	"com.typesafe" % "config" % "1.0.2",
	"org.mapdb" % "mapdb" % "0.9.8",
	"org.scalatest" % "scalatest_2.10" % "2.0.M6" % "test",
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


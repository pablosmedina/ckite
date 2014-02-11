name := "ckite"

organization := "ckite.io"

scalaVersion := "2.10.2"

resolvers += "twitter-repo" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.0.0",
	"com.twitter" %% "scrooge-core" % "3.9.0",
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



import AssemblyKeys._

name := "ckite"

scalaVersion := "2.10.2"

jarName in assembly := "ckite.jar"

EclipseKeys.withSource := true

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
   "org.apache.tomcat" % "tomcat-catalina" % "7.0.34",
   "org.apache.tomcat.embed" % "tomcat-embed-core" % "7.0.34",
   "org.apache.tomcat.embed" % "tomcat-embed-jasper" % "7.0.34",
   "org.springframework" % "spring-webmvc" % "3.1.0.RELEASE",
   "ch.qos.logback" % "logback-classic" % "1.0.0",
   "cglib" % "cglib-nodep" % "2.2",
   "com.esotericsoftware.kryo"     %  "kryo"           % "2.22",
   "org.apache.httpcomponents" % "httpclient" % "4.2.5",
   "org.scalatest" % "scalatest_2.10" % "2.0.M6" % "test",
   "junit" % "junit" % "4.8.1" % "test"
)

assemblySettings
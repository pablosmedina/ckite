import com.typesafe.sbteclipse.core.EclipsePlugin.{EclipseCreateSrc, EclipseKeys}
import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

object Settings {

  val ScalaVersion = "2.11.4"

  lazy val basicSettings = Seq(
    scalaVersion := ScalaVersion,
    organization := "io.ckite",
    version := "0.2.0-SNAPSHOT",
    fork in(Test, run) := true,
    javacOptions := Seq(
      "-source", "1.8", "-target", "1.8"
    ),
    scalacOptions := Seq(
      "-encoding",
      "utf8",
      "-g:vars",
      "-feature",
      "-unchecked",
      "-optimise",
      "-deprecation",
      "-target:jvm-1.8",
      "-language:postfixOps",
      "-language:implicitConversions",
      "-language:reflectiveCalls",
      "-Xlog-reflective-calls"
    )) ++ sonatypeSettings ++ com.twitter.scrooge.ScroogeSBT.newSettings

  lazy val sonatypeSettings = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := true,
    pomIncludeRepository := { x => false},
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
  )

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences
  )

  def formattingPreferences =
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, true)
      .setPreference(AlignParameters, false)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)

  lazy val eclipseSettings = Seq(EclipseKeys.configurations := Set(Compile, Test, IntegrationTest), EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource)
  lazy val itExtraSettings = Seq(
    parallelExecution in IntegrationTest := false
  )
}

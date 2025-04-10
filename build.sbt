import sbt._
import sbt.Keys._

scalaVersion := "3.6.4"

Compile / mainClass := Some("de.pc2.dedup.fschunk.trace.Main")
Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "gen"

lazy val fs_c = project
    .in(file("."))
    .settings(
      name := "FS-C",
      resolvers += Resolver.jcenterRepo,
//      javacOptions ++= Seq("-source", "23.0.2"),
      libraryDependencies ++= Seq(
        "com.github.scopt" %% "scopt" % "4.1.0",
        "commons-codec" % "commons-codec" % "1.18.0",
        "commons-lang" % "commons-lang" % "2.6",
        // versions of "commons-logging" >= 1.2 break program output
        // maybe changing logger would help
        "commons-logging" % "commons-logging" % "1.3.5",
        // versions of hazelcast >= 3.0 lead to compiler error
        // probably due to an API change
        "com.hazelcast" % "hazelcast" % "5.5.0",
        "log4j" % "log4j" % "1.2.17",
        //     "org.clapper" %% "argot" % "1.0.4",
        "org.scalatest" % "scalatest_2.9.0" % "1.9.2",
        "com.google.guava" % "guava" % "23.0",
        "org.apache.pig" % "pig" % "0.17.0",
        "org.apache.hadoop" % "hadoop-core" % "1.2.1"
      )
    )

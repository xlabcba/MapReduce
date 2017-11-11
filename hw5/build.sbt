name := "imagepreparation"

version := "1.0"

scalaVersion := "2.11.8"

// resolvers += "ImageIORepo" at "http://maven.geotoolkit.org/"

// resolvers += "ClibwrapperRepo" at "http://repo.boundlessgeo.com/main/"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
    "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided",
    "com.github.jai-imageio" % "jai-imageio-core" % "1.3.1"
    // "javax.media" % "jai_core" % "1.1.3",
    // "javax.media" % "jai_imageio" % "1.1",
    // "com.sun" % "clibwrapper_jiio" % "1.1"
)
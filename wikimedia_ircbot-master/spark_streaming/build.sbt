name := "wikimediabot"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.2.0"

libraryDependencies <<= scalaVersion {
  scala_version => Seq(

    ("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion),
    ("org.apache.spark" %% "spark-streaming" % sparkVersion % "provided").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.activation").
	exclude("com.esotericsoftware.kryo", "minlog").
	exclude("com.esotericsoftware.minlog", "minlog").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-logging", "commons-logging").
    exclude("org.slf4j", "jcl-over-slf4j").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
	exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.servlet"),
	("net.liftweb" %% "lift-json" % "2.5.1")

  )
}

resolvers += "typesafe repo" at " http://repo.typesafe.com/typesafe/releases/"

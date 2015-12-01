import AssemblyKeys._ // put this at the top of the file
assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case PathList("com", "twitter", xs @ _*) => MergeStrategy.last
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "pom.xml" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "overview.html" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "parquet.thrift" => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x => old(x)
  }
}

jarName in assembly := "loggerhead.jar"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

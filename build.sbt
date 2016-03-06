organization := "io.rs.peerstream"
scalaVersion := "2.11.7"

libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "0.9.0" exclude("org.apache.kafka", "kafka_${scala.binary.version}")
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"

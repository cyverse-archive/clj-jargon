(defproject org.iplantc/clj-jargon "0.1.0-SNAPSHOT"
  :description "Clojure API on top of iRODS's jargon-core."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.iplantc/clojure-commons "1.1.0-SNAPSHOT"]
                 [org.irods.jargon/jargon-core "3.0.1-SNAPSHOT"]
                 [org.irods.jargon/jargon-test "3.0.1-SNAPSHOT"]
                 [org.irods.jargon/jargon-data-utils "3.0.1-SNAPSHOT"]
                 [org.irods.jargon.transfer/jargon-transfer-engine "3.0.1-SNAPSHOT"]
                 [org.irods.jargon/jargon-security "3.0.1-SNAPSHOT"]]
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"
                 
                 "renci.repository"
                 "http://ci-dev.renci.org/nexus/content/repositories/snapshots/"
                 
                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases"})
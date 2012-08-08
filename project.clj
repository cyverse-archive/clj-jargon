(defproject org.iplantc/clj-jargon "0.1.1-SNAPSHOT"
  :description "Clojure API on top of iRODS's jargon-core."
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.iplantc/clojure-commons "1.1.0-SNAPSHOT"]
                 [org.irods.jargon/jargon-core "3.1.2"]
                 [org.irods.jargon/jargon-data-utils "3.1.2"]
                 [org.irods.jargon.transfer/jargon-transfer-engine "3.1.2"]
                 [org.irods.jargon/jargon-security "3.1.2"]
                 [org.clojure/tools.logging "0.2.3"]]
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"
                 
                 "renci.repository.snapshots"
                 "http://ci-dev.renci.org/nexus/content/repositories/snapshots/"
                 
                 "renci.repository.releases"
                 "http://ci-dev.renci.org/nexus/content/repositories/releases/"
                 
                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases"})

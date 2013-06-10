(defproject org.iplantc/clj-jargon "0.2.6-SNAPSHOT"
  :description "Clojure API on top of iRODS's jargon-core."
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.iplantc/clojure-commons "1.4.1-SNAPSHOT"]
                 [org.irods.jargon/jargon-core "3.3.2-SNAPSHOT"
                  :exclusions [[org.jglobus/JGlobus-Core]
                               [org.slf4j/slf4j-api]
                               [org.slf4j/slf4j-log4j12]]]
                 [org.irods.jargon/jargon-data-utils "3.3.2-SNAPSHOT"
                  :exclusions [[org.slf4j/slf4j-api]
                               [org.slf4j/slf4j-log4j12]]]
                 [org.irods.jargon.transfer/jargon-transfer-engine "3.3.2-SNAPSHOT"]
                 [org.irods.jargon/jargon-security "3.3.2-SNAPSHOT"]
                 [org.irods.jargon/jargon-ticket "3.3.2-SNAPSHOT"]
                 [slingshot "0.10.3"]
                 [org.clojure/tools.logging "0.2.6"]]
  :profiles {:dev {:dependencies [[org.iplantc/boxy "0.1.2-SNAPSHOT"]]}}
  :repositories
  {"iplantCollaborative"
   "http://projects.iplantcollaborative.org/archiva/repository/internal/"

   "renci.repository.releases"
   "http://ci-dev.renci.org/nexus/content/repositories/releases/"

   "renci.repository.snapshots"
   "http://ci-dev.renci.org/nexus/content/repositories/snapshots/"

   "sonatype"
   "http://oss.sonatype.org/content/repositories/releases"})

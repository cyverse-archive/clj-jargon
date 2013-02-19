(defproject org.iplantc/clj-jargon "0.2.3-SNAPSHOT"
  :description "Clojure API on top of iRODS's jargon-core."
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.iplantc/clojure-commons "1.3.0-SNAPSHOT"]
                 [org.irods.jargon/jargon-core "3.2.1.3"
                  :exclusions [org.jglobus/JGlobus-Core]]
                 [org.irods.jargon/jargon-data-utils "3.2.1.3"]
                 [org.irods.jargon.transfer/jargon-transfer-engine "3.2.1.3"]
                 [org.irods.jargon/jargon-security "3.2.1.3"]
                 [org.irods.jargon/jargon-ticket "3.2.1.3"]
                 [slingshot "0.10.1"]
                 [org.clojure/tools.logging "0.2.3"]]
  :profiles {:dev {:dependencies [[org.iplantc/boxy "0.1.0-SNAPSHOT"]]}}
  :repositories
  {"iplantCollaborative"
   "http://projects.iplantcollaborative.org/archiva/repository/internal/"

   "renci.repository.releases"
   "http://ci-dev.renci.org/nexus/content/repositories/releases/"

   "renci.repository.snapshots"
   "http://ci-dev.renci.org/nexus/content/repositories/snapshots/"

   "sonatype"
   "http://oss.sonatype.org/content/repositories/releases"})

(defproject org.iplantc/clojure-commons "1.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [zookeeper-clj "0.9.1"]
                 [clj-http "0.2.5"]
                 [log4j/log4j "1.2.16"]
                 [org.mongodb/mongo-java-driver "2.6.3"]
                 [org.apache.httpcomponents/httpclient "4.1.2"]
                 [commons-configuration/commons-configuration "1.7"]
                 [org.jasig.cas.client/cas-client-core "3.2.0"
                  :exclusions [javax.servlet/servlet-api]]]
  :dev-dependencies [[swank-clojure "1.3.2"]])

(defproject org.iplantc/clojure-commons "1.4.7"
  :description "Common Utilities for Clojure Projects"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [zookeeper-clj "0.9.1"]
                 [clj-http "0.6.5"]
                 [clj-http-fake "0.4.1"]
                 [com.cemerick/url "0.0.7"]
                 [com.github.drsnyder/beanstalk "1.0.0-clj14"]
                 [log4j/log4j "1.2.17"]
                 [slingshot "0.10.3"]
                 [org.mongodb/mongo-java-driver "2.10.1"]
                 [org.apache.httpcomponents/httpclient "4.2.3"]
                 [commons-configuration/commons-configuration "1.8"]
                 [cheshire "5.0.2"]]
  :profiles {:test {:resource-paths ["resources" "test-resources"]}})

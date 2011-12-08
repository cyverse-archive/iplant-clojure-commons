(ns clojure-commons.props
  (:use [clojure.java.io :only (file)])
  (:import java.net.URLDecoder))

(defn read-properties
  "Reads in properties from a file and instantiates a loaded Properties object.
   Adapted from code in the clojure.contrib.properties."
  [file-path]
  (with-open [f (java.io.FileInputStream. (file file-path))]
    (doto (java.util.Properties.)
      (.load f))))

(defn find-properties-file
  "Searches the classpath for the named properties file."
  [prop-name]
  (. (. (. (. Thread currentThread) getContextClassLoader) getResource prop-name) getFile))

(defn find-resources-file
  [filename]
  (find-properties-file filename))

(defn parse-properties
  [file-name]
  (let [prop-path (find-properties-file file-name)
        prop (if (nil? prop-path) (str "resources/" file-name) prop-path)]
    (read-properties (URLDecoder/decode prop))))


(ns clojure-commons.props
  (:use    [clojure.contrib.java-utils])
  (:import java.net.URLDecoder))

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


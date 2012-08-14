(ns clojure-commons.file-utils
  (:use [clojure.java.io :only [file]]
        [clojure.string :only [join split]])
  (:import [java.io File]))

(defn path-join
  "Joins paths together and returns the resulting path as a string."
  [path & paths]
  (if (seq paths)
    (let [path1    path
          path2    (first paths)
          new-path (str (file (file path1) path2))]
      (if (seq (rest paths))
        (recur new-path (rest paths))
        new-path))
    path))

(defn rm-last-slash
  "Returns a new version of 'path' with the last slash removed.

   Parameters:
     path - String containing a path.

   Returns: New version of 'path' with the trailing slash removed."
  [path]
  (.replaceAll path "/$" ""))

(defn basename
  "Returns the basename of 'path'.

   This works by calling getName() on a java.io.File instance. It's prefered
   over last-dir-in-path for that reason.

   Parameters:
     path - String containing the path for an item in iRODS.

   Returns:
     String containing the basename of path."
  [path]
  (.getName (file path)))

(defn dirname
  "Returns the dirname of 'path'.

   This works by calling getParent() on a java.io.File instance.

   Parameters:
     path - String containing the path for an item in iRODS.

   Returns:
     String containing the dirname of path."
  [path]
  (.getParent (file path)))

(defn add-trailing-slash
  "Adds a trailing slash to 'input-string' if it doesn't already have one."
  [input-string]
  (if-not (.endsWith input-string "/")
    (str input-string "/")
    input-string))

(defn normalize-path
  "Normalizes a file path on Unix systems by eliminating '.' and '..' from it.
   No attempts are made to resolve symbolic links."
  [file-path]
  (loop [dest [] src (split file-path #"/")]
    (if (empty? src)
      (join "/" dest)
      (let [curr (first src)]
        (cond (= curr ".") (recur dest (rest src))
              (= curr "..") (recur (vec (butlast dest)) (rest src))
              :else (recur (conj dest curr) (rest src)))))))

(defn abs-path
  "Converts a path to an absolute path."
  [file-path]
  (normalize-path (.getAbsolutePath (file file-path))))

(defn abs-path?
  "Returns true if the path passed in is an absolute path."
  [file-path]
  (.isAbsolute (file file-path)))

(defn file?
  "Tests whether the path is a file."
  [file-path]
  (.isFile (file file-path)))

(defn dir?
  "Tests whether the path is a directory."
  [file-path]
  (.isDirectory (file file-path)))

(defn exists?
  "Tests whether the given paths exist on the filesystem."
  [& filepaths]
  (every? #(.exists %) (map file filepaths)))

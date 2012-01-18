(ns clojure-commons.file-utils
  (:import [java.io File]))

(defn path-join
  "Joins paths together and returns the resulting path as a string."
  [path & paths]
  (if (seq paths)
    (let [path1    path
          path2    (first paths)
          new-path (. (java.io.File. (java.io.File. path1) path2) toString)]
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
  (. path replaceAll "/$" ""))

(defn basename
  "Returns the basename of 'path'.

   This works by calling getName() on a java.io.File instance. It's prefered
   over last-dir-in-path for that reason.

   Parameters:
     path - String containing the path for an item in iRODS.

   Returns:
     String containing the basename of path."
  [path]
  (. (File. path) getName))

(defn dirname
  "Returns the dirname of 'path'.

   This works by calling getParent() on a java.io.File instance.

   Parameters:
     path - String containing the path for an item in iRODS.

   Returns:
     String containing the dirname of path."
  [path]
  (. (File. path) getParent))

(defn add-trailing-slash
  "Adds a trailing slash to 'input-string' if it doesn't already have one."
  [input-string]
  (if (not (. input-string endsWith "/"))
    (str input-string "/")
    input-string))

(defn abs-path
  "Converts a path to an absolute path."
  [file-path]
  (. (File. file-path) getAbsolutePath))

(defn abs-path?
  "Returns true if the path passed in is an absolute path."
  [file-path]
  (. (File. file-path) isAbsolute))

(defn file?
  "Tests whether the path is a file."
  [file-path]
  (. (File. file-path) isFile))

(defn dir?
  "Tests whether the path is a directory."
  [file-path]
  (. (File. file-path) isDirectory))

(defn exists?
  "Tests whether the given paths exist on the filesystem."
  [& filepaths]
  (every? #(. % exists) (map #(File. %) filepaths)))
(ns clojure-commons.nibblonian
  (:use [clojure.data.json :only [json-str]]
        [clojure-commons.error-codes])
  (:require [clj-http.client :as client]
            [clojure-commons.client :as cc]))

(defn delete-avu
  "Removes an AVU from a file."
  [base user path attr]
  (let [url (cc/build-url base "file" "metadata")
        res (cc/delete url {:query-params {:path path
                                           :user user
                                           :attr attr}})]
    (:body res)))

(defn delete-tree-urls
  "Removes all of the tree URLs associated with a file."
  [base user path]
  (delete-avu base user path "tree-urls"))

(defn format-tree-url
  "Creates a tree URL element."
  [label url]
  {:label label
   :url   url})

(defn format-tree-urls
  "Formats the tree URLs for storage in the file metadata.  The urls argument
   should contain a sequence of elements as returned by format-tree-url."
  [urls]
  {:tree-urls urls})

(defn save-tree-urls
  "Saves tree URLs in the file metadata.  The urls argument should contain a
   sequence of elements as returned by format-tree-url."
  [base user path urls]
  (let [url  (cc/build-url base "file" "tree-urls")
        body (json-str (format-tree-urls urls))
        res  (cc/post url {:body         body
                           :content-type :json
                           :query-params {:path path
                                          :user user}})]
    (:body res)))

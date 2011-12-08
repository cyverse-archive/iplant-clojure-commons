(ns clojure-commons.osm
  (:use [clojure.string :only (join)])
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]))

;; Creates a new OSM client.
(defn create [base bucket]
  {:base base
   :bucket bucket})

;; Builds an OSM URL from the base URL, the bucket and the list of components.
(defn- build-url [osm & components]
  (join "/" (map #(.replaceAll % "^/|/$" "")
                 (concat [(:base osm) (:bucket osm)] components))))

;; Sends a query to the OSM.
(defn query [osm query]
  (:body (client/post (build-url osm "query")
                      {:body (json/json-str query)})))

;; Saves an object in the OSM.
(defn save-object [osm obj]
  (:body (client/post (build-url osm) {:body (json/json-str obj)})))

;; Gets an object from the OSM.
(defn get-object [osm id]
  (:body (client/get (build-url osm id))))

;; Updates an object in the OSM.
(defn update-object [osm id obj]
  (:body (client/post (build-url osm id) {:body (json/json-str obj)})))

;; Adds a callback to an object in the OSM.
(defn add-callback [osm id type url]
  (:body (client/post
           (build-url osm id "callbacks")
           {:body (json/json-str {:callbacks [{:type type :callback url}]})})))

;; Removes a calback from an object in the OSM.
(defn remove-callback [osm id type url]
  (:body (client/post
           (build-url osm id "callbacks" "delete")
           {:body (json/json-str {:callbacks [{:type type :callback url}]})})))

;; Gets the list of callbacks from the OSM.
(defn get-callbacks [osm id]
  (:body (client/get (build-url osm id))))

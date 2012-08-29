(ns clojure-commons.client
  (:use [clojure.string :only [join]]
        [slingshot.slingshot :only [throw+]])
  (:require [clj-http.client :as client]
            [clojure-commons.error-codes :as ce])
  (:import [java.io InputStream])
  (:refer-clojure :exclude [get]))

(defn build-url
  "Builds a URL from multiple component strings."
  [& components]
  (join "/" (map #(.replaceAll % "^/|/$" "") components)))

(defn- wrap-req
  "Wraps a request so that we can throw our own exception rather than catch and
   rethrow the exception that clj-http.client throws."
  [f url & [req]]
  (let [stream? #(instance? InputStream %)
        res     (f url (assoc req :throw-exceptions false))
        status  (:status res)
        body    (:body res)]
    (when-not (< 199 status 300)
      (throw+ {:error_code ce/ERR_REQUEST_FAILED
               :body       (if (stream? body) (slurp body) body)}))
    res))

;; Common HTTP requests that throw customized exceptions.
(def get     (partial wrap-req client/get))
(def head    (partial wrap-req client/head))
(def post    (partial wrap-req client/post))
(def put     (partial wrap-req client/put))
(def delete  (partial wrap-req client/delete))
(def options (partial wrap-req client/options))
(def copy    (partial wrap-req client/copy))
(def move    (partial wrap-req client/move))
(def patch   (partial wrap-req client/patch))

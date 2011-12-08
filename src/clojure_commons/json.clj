(ns clojure-commons.json
  (:use clojure.contrib.json
        [clojure.contrib.duck-streams :only (slurp*)]))

;;; Things to make working with POSTs/PUTs easier
(defonce json-mime-type "application/json")

(def string->json read-json)

(defn body->json
  "Takes in input from a post body and slurps/parses it."
  ([body]
     (body->json body true))
  ([body keywordize?]
     (string->json (slurp* body) keywordize?)))

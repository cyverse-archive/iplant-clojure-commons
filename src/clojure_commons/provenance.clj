(ns clojure-commons.provenance
  (:use cemerick.url)
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]))

(defn prov-map
  "Creates a map that can be passed to the provenance related functions.
   The proxy-user and data keys area optional and will be set to nil if they're
   not provided."
  [url id user service event category & [proxy-user data]]
  (hash-map
   :prov-url url
   :object-id id
   :user user
   :service service
   :event event
   :category category
   :proxy-user proxy-user
   :data data))

(defn dissoc-nils
  "Removes entries with a nil value from a map."
  [dmap]
  (into {} (filter (comp not nil? second) (seq dmap))))

(defn clean-prov-map
  "Removes the :prov-url value from the map, along with any keys with nil
   values."
  [prov-map]
  (-> prov-map
      (dissoc :prov-url)
      dissoc-nils))

(defn call-prov-log
  [prov-url lp-map]
  (client/put
   prov-url
   {:throw-exceptions false}
   {:content-type :json
    :body (json/json-str lp-map)}))

(defn call-add-object
  [prov-url ao-map]
  (client/put
   prov-url
   {:throw-exceptions false
    :content-type :json
    :body (json/json-str ao-map)}
   {:content-type :json
    :body (json/json-str ao-map)}))

(defn call-lookup-object
  [prov-url object-id]
  (client/get
   (str (url prov-url object-id))
   {:throw-exceptions false}))

(defn log-map
  [lmap line-header]
  (log/info (str line-header (json/json-str lmap)))
  lmap)

(defn log
  "Takes in a map created by (prov-map) and logs it to the provenance service."
  [{:keys [prov-url object-id user service event category proxy-user data]
    :as log-map}]
  (let [lp-map (clean-prov-map log-map)]    
    (log/info (str "Logging Provenance: " (json/json-str lp-map)))
    (log/info (str "Logging Provenance Response: "
                   (call-prov-log prov-url lp-map)))))

(defn register
  "Takes in an identifier string, an object name, and a optionally a string
   description. Returns a UUID."
  [prov-url obj-id name desc]
  (let [robj {:id obj-id :name name :desc desc}]
    (log/info (str "Register Provenance Object: " (json/json-str robj)))
    (-> (call-add-object prov-url robj)
        :body
        (log-map "Register Provenance Object Response: ")
        json/read-json)))

(defn lookup
  "Takes in an identifier and looks up the UUID."
  [prov-url obj-id]
  (let [resp (:body (call-lookup-object prov-url obj-id))]
    (println resp)
    (if (contains? resp :UUID)
      (:UUID resp)
      nil)))

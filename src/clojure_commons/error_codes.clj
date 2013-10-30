(ns clojure-commons.error-codes
  (:use [slingshot.slingshot :only [try+]])
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as cheshire]
            [clojure.string :as string]))

(defmacro deferr
  [sym]
  `(def ~sym (name (quote ~sym))))

(deferr ERR_DOES_NOT_EXIST)
(deferr ERR_EXISTS)
(deferr ERR_NOT_WRITEABLE)
(deferr ERR_NOT_READABLE)
(deferr ERR_WRITEABLE)
(deferr ERR_READABLE)
(deferr ERR_NOT_A_USER)
(deferr ERR_NOT_A_FILE)
(deferr ERR_NOT_A_FOLDER)
(deferr ERR_IS_A_FILE)
(deferr ERR_IS_A_FOLDER)
(deferr ERR_INVALID_JSON)
(deferr ERR_BAD_OR_MISSING_FIELD)
(deferr ERR_NOT_AUTHORIZED)
(deferr ERR_MISSING_QUERY_PARAMETER)
(deferr ERR_MISSING_FORM_FIELD)
(deferr ERR_NOT_AUTHORIZED)
(deferr ERR_BAD_QUERY_PARAMETER)
(deferr ERR_INCOMPLETE_DELETION)
(deferr ERR_INCOMPLETE_MOVE)
(deferr ERR_INCOMPLETE_RENAME)
(deferr ERR_REQUEST_FAILED)
(deferr ERR_UNCHECKED_EXCEPTION)
(deferr ERR_NOT_OWNER)
(deferr ERR_INVALID_COPY)
(deferr ERR_INVALID_URL)
(deferr ERR_TICKET_EXISTS)
(deferr ERR_TICKET_DOES_NOT_EXIST)
(deferr ERR_MISSING_DEPENDENCY)
(deferr ERR_CONFIG_INVALID)
(deferr ERR_ILLEGAL_ARGUMENT)
(deferr ERR_BAD_REQUEST)
(deferr ERR_NOT_FOUND)

(def ^:private http-status-for
  {ERR_ILLEGAL_ARGUMENT 400
   ERR_INVALID_JSON     400
   ERR_BAD_REQUEST      400
   ERR_NOT_FOUND        404})

(defn- get-http-status
  [err-code]
  (get http-status-for err-code 500))

(defn error?
  [obj]
  (try
    (contains? obj :error_code)
    (catch Exception e
      false)))

(defn unchecked [throwable-map]
  {:error_code ERR_UNCHECKED_EXCEPTION
   :message (:message throwable-map)})

(defn err-resp
  ([err-obj]
     {:status (get-http-status (:error_code err-obj))
      :body (-> err-obj
                (assoc :status "failure")
                cheshire/encode)})
  ([action err-obj]
     {:status (get-http-status (:error_code err-obj))
      :body (-> err-obj
                (assoc :action action)
                (assoc :status "failure")
                cheshire/encode)}))

(defn success-resp [action retval]
  (cond
   (= (:status retval) 200)
   retval

   (= (:status retval) 404)
   retval

   :else
   {:status 200
    :body
    (cond
     (map? retval)
     (-> retval
         (assoc :status "success"
                :action action)
         cheshire/encode)

     (not (string? retval))
     (str retval)

     :else retval)}))

(def filters (ref #{}))

(defn register-filters
  [new-filters]
  (dosync
    (ref-set filters (set (concat @filters new-filters)))))

(defn log-filters [] @filters)

(defn format-exception
  "Formats the exception as a string."
  [exception]
  (let [string-writer  (java.io.StringWriter.)
        print-writer   (java.io.PrintWriter. string-writer)]
    (.printStackTrace exception print-writer)
    (let [stack-trace (str string-writer)]
      (reduce #(string/replace %1 %2 "xxxxxxxx") 
              (cons (str string-writer) (log-filters))))))

(defn trap [action func & args]
  (try+
    (success-resp action (apply func args))
    (catch error? err
      (log/error (format-exception (:throwable &throw-context)))
      (err-resp action (:object &throw-context)))
    (catch java.lang.Exception e
      (log/error (format-exception (:throwable &throw-context)))
      (err-resp action (unchecked &throw-context)))))

(defn wrap-errors
  "Ring handler for formatting errors that sneak by (trap). For instance
   errors that occur in other Ring handlers."
  [handler]
  (fn [req]
    (try+
      (handler req)
      (catch java.lang.Exception e
        (log/error (format-exception (:throwable &throw-context)))
        (err-resp (unchecked &throw-context))))))

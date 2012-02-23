(ns clojure-commons.cas-proxy-auth
  (:use [clojure.string :only (blank?)])
  (:require [clojure.tools.logging :as log])
  (:import [org.jasig.cas.client.validation Cas20ProxyTicketValidator
            TicketValidationException]))

(defn- get-assertion
  "Gets a security assertion from the CAS server."
  [proxy-ticket cas-server server-name]
  (log/debug proxy-ticket)
  (if (not (blank? proxy-ticket))
    (let [validator (Cas20ProxyTicketValidator. cas-server)]
      (.setAcceptAnyProxy validator true)
      (try
        (.validate validator proxy-ticket server-name)
        (catch TicketValidationException e
          (do (log/error e "proxy ticket validation failed") nil))))
    nil))

(defn- build-attr-map
  "Builds a map containing the user's attributes"
  [principal]
  (assoc
    (into {} (.getAttributes principal))
    "uid" (.getName principal)))

(defn- assoc-attrs
  "Associates user attributes from an assertion principal with a request."
  [request principal]
  (let [m (build-attr-map principal)]
    (log/debug "User Attributes:" m)
    (assoc request :user-attributes m)))

(defn validate-cas-proxy-ticket
  "Authenticates a CAS proxy ticket that has been sent to the service in a
   query string parameter called, proxyToken.  If the proxy ticket can be
   validated then the request is passed to the handler.  Otherwise, the
   handler responds with HTTP status code 401.

   This handler currently relies upon two configuration parameters:
   conrad.cas.server and conrad.server-name."
  [handler cas-server-fn server-name-fn]
  (fn [request]
    (let [ticket (get (:query-params request) "proxyToken")]
      (log/debug (str "validating proxy ticket: " ticket))
      (let [assertion (get-assertion ticket (cas-server-fn) (server-name-fn))]
        (if (nil? assertion)
          {:status 401}
          (handler (assoc-attrs request (.getPrincipal assertion))))))))

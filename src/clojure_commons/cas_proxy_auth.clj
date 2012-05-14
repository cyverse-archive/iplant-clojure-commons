(ns clojure-commons.cas-proxy-auth
  (:use [clojure.string :only (blank? split)])
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

(defn- string->vector
  "Converts a string representation of a Java string array to a vector."
  [string]
  (let [groups (re-find #"\A\[([^\]]*)\]\z" string)]
    (if (or (nil? groups) (blank? (second groups)))
      []
      (split (second groups) #",\s*"))))

(defn validate-group-membership
  "Verifies that a the user belongs to at least one group that is permitted to
   access a resource."
  [handler allowed-groups-fn]
  (fn [request]
    (let [allowed-groups (allowed-groups-fn)
          actual-groups (:user-groups request)]
      (log/debug "allowed groups:" allowed-groups)
      (log/debug "actual groups:" actual-groups)
      (if (some #(contains? (set allowed-groups) %) actual-groups)
        (handler request)
        {:status 401}))))

(defn extract-groups-from-user-attributes
  "Extracts group membership information from the user's attributes.  The
   group membership information is assumed to be in a format that resembles
   the list representation of an array of strings in Java."
  [handler attr-name-fn]
  (fn [request]
    (let [attr-value (get-in request [:user-attributes (attr-name-fn)])]
      (log/debug "group membership attribute value: " attr-value)
      (handler (assoc request :user-groups (string->vector attr-value))))))

(defn validate-cas-proxy-ticket
  "Authenticates a CAS proxy ticket that has been sent to the service in a
   query string parameter called, proxyToken.  If the proxy ticket can be
   validated then the request is passed to the handler.  Otherwise, the
   handler responds with HTTP status code 401."
  [handler cas-server-fn server-name-fn]
  (fn [request]
    (let [ticket (get (:query-params request) "proxyToken")]
      (log/debug (str "validating proxy ticket: " ticket))
      (let [assertion (get-assertion ticket (cas-server-fn) (server-name-fn))]
        (if (nil? assertion)
          {:status 401}
          (handler (assoc-attrs request (.getPrincipal assertion))))))))

(defn validate-cas-group-membership
  "This is a convenience function that produces a handler that validates a
   CAS ticket, extracts the group membership information from a user
   attribute and verifies that the user belongs to one of the groups
   that are permitted to access the resource."
  [handler cas-server-fn server-name-fn attr-name-fn allowed-groups-fn]
  (-> handler
    (validate-group-membership allowed-groups-fn)
    (extract-groups-from-user-attributes attr-name-fn)
    (validate-cas-proxy-ticket cas-server-fn server-name-fn)))

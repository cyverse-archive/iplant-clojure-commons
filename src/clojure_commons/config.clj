(ns clojure-commons.config
  (:use [clojure.java.io :only [file]]
        [slingshot.slingshot :only [throw+]])
  (:require [clojure.string :as string]
            [clojure-commons.clavin-client :as cl]
            [clojure-commons.error-codes :as ce]
            [clojure-commons.props :as cp]
            [clojure.tools.logging :as log])
  (:import [java.io IOException]))

(def ^:private zkhosts-path "/etc/iplant-services/zkhosts.properties")

(defn get-zk-url
  "Gets the Zookeeper connection information from the standard location for iPlant services.  In
   cases where the calling service wants to check for the presence of the configuration in order
   to determine whether the properties should be loaded from Zookeeper or a local configuration
   file, failure to load the Zookeeper connection information may not be an error.  Because of
   this, if the connection information can't be loaded, this function logs a warning message and
   returns nil."
  []
  (try
    (get (cp/read-properties zkhosts-path) "zookeeper")
    (catch IOException e
      (log/warn e "unable to load Zookeeper properties")
      nil)))

(defn load-configuration-from-file
  "Loads the configuration properties from a file.

   Parameters:
       conf-dir - the path to the configuration directory.
       filename - the name of the configuration file.
       props    - the ref or atom to store the properties in."
  [conf-dir filename props]
  (if (nil? conf-dir)
    (reset! props (cp/read-properties (file filename)))
    (reset! props (cp/read-properties (file conf-dir filename)))))

(defn load-configuration-from-zookeeper
  "Loads the configuration properties from Zookeeper.  If the Zookeeper connection information
   is specified then that connection information will be used.  Otherwise, the connection
   information will be obtained from the zkhosts.properties file.

   Parameters:
       zk-url  - the URL used to connect to connect to Zookeeper (optional).
       props   - a ref or atom to store the properties in.
       service - the name of the service."
  ([props service]
     (let [zk-url (get-zk-url)]
       (when (nil? zk-url)
         (throw+ {:error_code ce/ERR_MISSING_DEPENDENCY
                  :detail_msg "iplant-services is not installed"}))
       (load-configuration-from-zookeeper zk-url props service)))
  ([zk-url props service]
     (cl/with-zk
       zk-url
       (when (not (cl/can-run?))
         (log/warn "THIS APPLICATION CANNOT RUN ON THIS MACHINE. SO SAYETH ZOOKEEPER.")
         (log/warn "THIS APPLICATION WILL NOT EXECUTE CORRECTLY.")
         (System/exit 1))
       (reset! props (cl/properties service)))))

(defn record-missing-prop
  "Records a property that is missing.  Instead of failing on the first missing parameter, we log
   the missing parameter, mark the configuration as invalid and keep going so that we can log as
   many configuration errors as possible in one run.

   Parameters:
       prop-name    - the name of the property.
       config-valid - a ref or atom containing a validity flag."
  [prop-name config-valid]
  (log/error "required configuration setting" prop-name "is empty or"
             "undefined")
  (reset! config-valid false))

(defn record-invalid-prop
  "Records a property that has an invalid value.  Instead of failing on the first missing
   parameter, we log the missing parameter, mark the configuration as invalid and keep going so
   that we can log as many configuration errors as possible in one run.

   Parameters:
       prop-name    - the name of the property.
       t            - the Throwable instance that caused the error.
       confiv-valid - a ref or atom containing a validity flag."
  [prop-name t config-valid]
  (log/error "invalid configuration setting for" prop-name ":" t)
  (reset! config-valid false))

(defn get-required-prop
  "Gets a required property from a set of properties.

   Parameters:
       props        - a ref or atom containing the properties.
       prop-name    - the name of the property.
       config-valid - a ref or atom containing a validity flag."
  [props prop-name config-valid]
  (let [value (get @props prop-name "")]
    (when (string/blank? value)
      (record-missing-prop prop-name config-valid))
    value))

(defn get-optional-prop
  "Gets an optional property from a set of properties.

   Parameters:
       props        - a ref or atom containing the properties.
       prop-name    - the name of the property.
       config-valid - a ref or atom containing a validity flag."
  [props prop-name config-valid]
  (get @props prop-name ""))

(defn vector-from-prop
  "Derives a list of values from a single comma-delimited value.

   Parameters:
       value - the value to convert to a vector."
  [value]
  (string/split value #", *"))

(defn get-required-vector-prop
  "Gets a required vector property from a set of properties.

   Parameters:
       props        - a ref or atom containing the properties.
       prop-name    - the name of the property.
       config-valid - a ref or atom containing a validity flag."
  [props prop-name config-valid]
  (vector-from-prop (get-required-prop props prop-name config-valid)))

(defn string-to-int
  "Attempts to convert a String property to an integer property.  Returns zero if the property
   can't be converted.

   Parameters:
       prop-name    - the name of the property.
       value        - the value of the property as a string.
       config-valid - a ref or atom containing a vailidity flag."
  [prop-name value config-valid]
  (try
    (Integer/parseInt value)
    (catch NumberFormatException e
      (record-invalid-prop prop-name e config-valid)
      0)))

(defn string-to-long
  "Attempts to convert a String property to a long property.  Returns zero if the property can't
   be converted.

   Parameters:
       prop-name    - the name of the property.
       value        - the value of the property as a string.
       config-valid - a ref or atom containing a validity flag."
  [prop-name value config-valid]
  (try
    (Long/parseLong value)
    (catch NumberFormatException e
      (record-invalid-prop prop-name e config-valid)
      0)))

(defn get-required-integer-prop
  "Gets a required integer property from a set of properties.  If the property is missing or not
   able to be converted to an integer then the configuration will be marked as invalid and zero
   will be returned.

   Parameters:
       props        - a ref or atom containing the properties.
       prop-name    - the name of the property.
       config-valid - a ref or atom containing a validity flag."
  [props prop-name config-valid]
  (let [value (get-required-prop props prop-name config-valid)]
    (if (string/blank? value)
      0
      (string-to-int prop-name value config-valid))))

(defn get-required-long-prop
  "Gets a required long property from a set of properties.  If a property is missing or not able to
   be converted to a long then the configuration will be marked as invalid and zero will be
   returned.

   Parameters:
       props        - a ref or atom containing the properties.
       prop-name    - the name of a property.
       config-valid - a ref or atom containing a validity flag."
  [props prop-name config-valid]
  (let [value (get-required-prop props prop-name config-valid)]
    (if (string/blank? value)
      0
      (string-to-long prop-name value config-valid))))

(defmacro defprop-str
  "defines a required string property.

   Parameters:
       sym          - the symbol to define.
       desc         - a brief description of the property.
       props        - a ref or atom containing the properties.
       config-valid - a ref or atom containing a validity flag.
       prop-name    - the name of the property."
  [sym desc [props config-valid] prop-name]
  `(defn ~sym ~desc [] (get-required-prop ~props ~prop-name ~config-valid)))

(defmacro defprop-optstr
  "Defines an optional string property.

   Parameters:
       sym          - the symbol to define.
       desc         - a brief description of the property.
       props        - a ref or atom containing the properties.
       config-valid - a ref or atom containing a validity flag.
       prop-name    - the name of the property."
  [sym desc [props config-valid] prop-name]
  `(defn ~sym ~desc [] (get-optional-prop ~props ~prop-name ~config-valid)))

(defmacro defprop-vec
  "Defines a required vector property.

   Parameters:
       sym          - the symbol to define.
       desc         - a brief description of the property.
       props        - a ref or atom containing the properties.
       config-valid - a ref or atom containing a validity flag.
       prop-name    - the name of the property."
  [sym desc [props config-valid] prop-name]
  `(defn ~sym ~desc [] (get-required-vector-prop ~props ~prop-name ~config-valid)))

(defmacro defprop-int
  "Defines a required integer property.

   Parameters:
       sym          - the symbol to define.
       desc         - a brief description of the property.
       props        - a ref or atom containing the properties.
       config-valid - a ref or atom containing a validity flag.
       prop-name    - the name of the property."
  [sym desc [props config-valid] prop-name]
  `(defn ~sym ~desc [] (get-required-integer-prop ~props ~prop-name ~config-valid)))

(defmacro defprop-long
  "Defines a required long property.

   Parameters:
       sym          - the symbol to define.
       desc         - a brief description of the property.
       props        - a ref or atom containing the properties.
       config-valid - a ref or atom containing a validity flag.
       prop-name    - the name of the property."
  [sym desc [props config-valid] prop-name]
  `(defn ~sym ~desc [] (get-required-long-prop ~props ~prop-name ~config-valid)))

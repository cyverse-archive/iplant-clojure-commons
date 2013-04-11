(ns clojure-commons.config-test
  (:use [clojure.test]
        [clojure-commons.config])
  (:import [java.util Properties]))

(defn props-from-map
  "Creates an instance of java.util.Properties from a map."
  [m]
  (let [props (Properties.)]
    (dorun (map (fn [[k v]] (.setProperty props k v)) m))
    props))

;; Initial property definitions.

(def props
  "The example properties to use for testing."
  (ref (props-from-map
        {"foo"                      "bar"
         "baz"                      "quux"
         "defined-optional-string"  "blarg"
         "required-vector"          "foo, bar, baz"
         "defined-optional-vector"  "baz, bar, foo"
         "required-int"             "27"
         "defined-optional-int"     "53"
         "required-long"            "72"
         "defined-optional-long"    "35"
         "required-boolean"         "true"
         "defined-optional-boolean" "true"})))

(def config-valid
  "A flag indicating that the configuration is valid."
  (ref true))

(def configs
  "A vector of configuration settings."
  (ref []))

(defprop-str foo
  "Description of foo."
  [props config-valid configs]
  "foo")

(defprop-str baz
  "Description of baz."
  [props config-valid configs]
  "baz")

(defprop-optstr defined-optional-string
  "Defined optional string."
  [props config-valid configs]
  "defined-optional-string")

(defprop-optstr undefined-optional-string
  "Undefined optional string."
  [props config-valid configs]
  "undefined-optional-string")

(defprop-optstr undefined-optional-string-with-default
  "Undefined optional string with default value."
  [props config-valid configs]
  "undefined-optional-string-with-default"
  "The foo is in the bar.")

(defprop-vec required-vector
  "Required vector."
  [props config-valid configs]
  "required-vector")

(defprop-optvec defined-optional-vector
  "Defined optional vector."
  [props config-valid configs]
  "defined-optional-vector")

(defprop-optvec undefined-optional-vector
  "Undefined optional vector."
  [props config-valid configs]
  "undefined-optional-vector")

(defprop-optvec undefined-optional-vector-with-default
  "Undefined optional vector with default value."
  [props config-valid configs]
  "undefined-optional-vector-with-default"
  (mapv str (range 5)))

(defprop-int required-int
  "Required integer."
  [props config-valid configs]
  "required-int")

(defprop-optint defined-optional-int
  "Defined optional integer."
  [props config-valid configs]
  "defined-optional-int")

(defprop-optint undefined-optional-int
  "Undefined optional integer."
  [props config-valid configs]
  "undefined-optional-int")

(defprop-optint undefined-optional-int-with-default
  "Undefined optional integer with default value."
  [props config-valid configs]
  "undefined-optional-int-with-default"
  42)

(defprop-long required-long
  "Required long integer."
  [props config-valid configs]
  "required-long")

(defprop-optlong defined-optional-long
  "Defined optional long integer."
  [props config-valid configs]
  "defined-optional-long")

(defprop-optlong undefined-optional-long
  "Undefined optional long integer."
  [props config-valid configs]
  "undefined-optional-long")

(defprop-boolean required-boolean
  "Required boolean."
  [props config-valid configs]
  "required-boolean")

(defprop-optboolean defined-optional-boolean
  "Defined optional boolean."
  [props config-valid configs]
  "defined-optional-boolean")

(defprop-optboolean undefined-optional-boolean
  "Undefined optional boolean."
  [props config-valid configs]
  "undefined-optional-boolean")

(defprop-optboolean undefined-optional-boolean-with-default
  "Undefined optional boolean with default value."
  [props config-valid configs]
  "undefined-optional-boolean-with-default"
  true)

(deftest foo-defined
  (is (= "bar" (foo))))

(deftest baz-defined
  (is (= "quux" (baz))))

(deftest defined-optional-string-defined
  (is (= "blarg" (defined-optional-string))))

(deftest undefined-optional-string-empty
  (is (= "" (undefined-optional-string))))

(deftest undefined-optional-string-with-default-correct
  (is (= "The foo is in the bar." (undefined-optional-string-with-default))))

(deftest required-vector-defined
  (is (= ["foo" "bar" "baz"] (required-vector))))

(deftest defined-optional-vector-correct
  (is (= ["baz" "bar" "foo"] (defined-optional-vector))))

(deftest undefined-optional-vector-correct
  (is (= [] (undefined-optional-vector))))

(deftest undefined-optional-vector-with-default-correct
  (is (= (mapv str (range 5)) (undefined-optional-vector-with-default))))

(deftest required-in-defined
  (is (= 27 (required-int))))

(deftest defined-optional-int-correct
  (is (= 53 (defined-optional-int))))

(deftest undefined-optional-int-correct
  (is (zero? (undefined-optional-int))))

(deftest undefined-optional-int-with-default-correct
  (is (= 42 (undefined-optional-int-with-default))))

(deftest required-long-defined
  (is (= 72 (required-long))))

(deftest defined-optional-long-correct
  (is (= 35 (defined-optional-long))))

(deftest undefined-optional-long-correct
  (is (zero? (undefined-optional-long))))

(deftest required-boolean-defined
  (is (true? (required-boolean))))

(deftest defined-optional-boolean-defined
  (is (true? (defined-optional-boolean))))

(deftest undefined-optional-boolean-correct
  (is (false? (undefined-optional-boolean))))

(deftest undefined-optional-boolean-with-default-correct
  (is (true? (undefined-optional-boolean-with-default))))

(deftest configs-defined
  (is (= [#'foo
          #'baz
          #'defined-optional-string
          #'undefined-optional-string
          #'undefined-optional-string-with-default
          #'required-vector
          #'defined-optional-vector
          #'undefined-optional-vector
          #'undefined-optional-vector-with-default
          #'required-int
          #'defined-optional-int
          #'undefined-optional-int
          #'undefined-optional-int-with-default
          #'required-long
          #'defined-optional-long
          #'undefined-optional-long
          #'required-boolean
          #'defined-optional-boolean
          #'undefined-optional-boolean
          #'undefined-optional-boolean-with-default]
         @configs)))

(deftest initial-configs-valid
  (is (validate-config configs config-valid)))

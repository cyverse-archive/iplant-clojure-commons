(ns clojure-commons.infosquito.work-queue-test
  (:use clojure.test
        clojure-commons.infosquito.work-queue
        clojure-commons.infosquito.mock-beanstalk)
  (:require [clojure.data.json :as json]
            [slingshot.slingshot :as ss]))


(defn- init-tubes
  [queue-state id payload]
  (assoc queue-state :tubes {"infosquito" [{:id id :payload payload}]}))


(defn- init-client
  [& params]
  (mk-client #(apply mk-mock-beanstalk params) 1 2 "infosquito"))


(deftest test-mk-client
  (let [ctor   (fn [])
        client (mk-client ctor 1 2 "infosquito")]
    (is (= ctor (:ctor client)))
    (is (= 1 (:conn-tries client)))
    (is (= 2 (:job-ttr client)))
    (is (= "infosquito" (:tube client)))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server
  (let [client (init-client)]
    (with-server client
      (is (not= nil @(:beanstalk client))))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server-bad-connection
   (let [client (init-client (atom (assoc default-state :closed? true)))
         thrown (ss/try+
                  (with-server client)
                  false
                  (catch [:type :connection] {:keys []}
                    true))]
     (is thrown)))


(deftest test-delete
  (let [state  (atom (init-tubes default-state 0 (json/json-str {})))
        client (init-client state)]
    (with-server client (delete client 0))
    (is (empty? (get "default" (:queue @state))))))


(deftest test-delete-bad-connection
  (let [state  (atom (assoc (init-tubes default-state 0 (json/json-str {}))
                            :closed? true))
        client (init-client state)
        thrown (ss/try+
                 (with-server client
                   (swap! state #(assoc % :closed? true))
                   (delete client 0))
                 false
                 (catch [:type :connection] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put
  (let [state   (atom default-state)
        client  (init-client state)
        payload (json/json-str {})]
    (with-server client (put client payload))
    (is (= (get (:tubes @state) "infosquito")
           [{:id 0 :payload payload}]))))


(deftest test-put-oom
  (let [state  (atom (assoc default-state :oom? true))
        client (init-client state)
        thrown (ss/try+
                 (with-server client (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-oom] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put-drain
  (let [state  (atom (assoc default-state :draining? true))
        client (init-client state)
        thrown (ss/try+
                 (with-server client (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-draining] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put-bury
  (let [state  (atom (assoc default-state :bury? true))
        client (init-client state)
        thrown (ss/try+
                 (with-server client (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-oom] {:keys []}
                   true))]
    (is thrown)))


(deftest test-reserve
  (let [payload (json/json-str {})
        state   (atom (init-tubes default-state 0 payload))
        client  (init-client state)]
    (with-server client
      (is (= payload
             (:payload (reserve client)))))))

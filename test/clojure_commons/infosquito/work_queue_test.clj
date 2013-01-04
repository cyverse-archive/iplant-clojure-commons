(ns clojure-commons.infosquito.work-queue-test
  (:use clojure.test
        clojure-commons.infosquito.work-queue
        clojure-commons.infosquito.mock-beanstalk)
  (:require [clojure.data.json :as json]
            [slingshot.slingshot :as ss]))


(defn- init-tubes
  [queue-state id payload]
  (assoc queue-state :tubes {"infosquito" {:ready    (list (mk-job id 10 payload))
                                           :reserved #{}}}))


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
  (let [job-id 0
        job    (mk-job job-id 10 (json/json-str {}))
        tubes  {"infosquito" {:ready '() :reserved #{(mk-reservation job 0)}}}
        state  (atom (assoc default-state :tubes tubes))
        client (init-client state)]
    (with-server client (delete client job-id))
    (is (empty? (:reserved (get (:tubes @state) "infosquito"))))
    (is (empty? (:ready (get (:tubes @state) "infosquito"))))))

  
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
    (is (= (-> @state :tubes (get "infosquito") :ready)
           [(mk-job 0 2 payload)]))))


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


(deftest test-touch
  (let [job-id 0
        job    (mk-job job-id 10 (json/json-str {}))
        tubes  {"infosquito" {:ready '() :reserved #{(mk-reservation job 0)}}}
        now    1
        state  (atom (assoc default-state :tubes tubes :now now))
        client (init-client state)]
    (with-server client
      (touch client job-id))
    (is (= (-> @state :tubes (get "infosquito") :reserved)
           #{(mk-reservation job now)}))))


(deftest test-release
  (let [job-id 0
        job    (mk-job job-id 10 (json/json-str {}))
        tubes  {"infosquito" {:ready '() :reserved #{(mk-reservation job 0)}}}
        state  (atom (assoc default-state :tubes tubes))
        client (init-client state)]
    (with-server client
      (release client job-id))
    (is (= (-> @state :tubes (get "infosquito"))
           {:ready (list job) :reserved #{}}))))


(deftest test-release-bury
  (let [job-id 0
        job    (mk-job job-id 10 (json/json-str {}))
        tubes  {"infosquito" {:ready '() :reserved #{(mk-reservation job 0)}}}
        client (init-client (atom (assoc default-state :tubes tubes :bury? true)))
        thrown (ss/try+
                 (with-server client (release client job-id))
                 false
                 (catch [:type :beanstalkd-oom] {} true))]
    (is thrown)))

(ns clojure-commons.infosquito.work-queue-test
  (:use clojure.test
        clojure-commons.infosquito.work-queue
        clojure-commons.infosquito.mock-beanstalk)
  (:require [slingshot.slingshot :as ss]))


(def ^{:private true} JOB-TTR 10)


(defn- init-tubes
  [queue-state payload]
  (-> queue-state 
    (use-tube "infosquito") 
    (put-job JOB-TTR payload)))
 

(defn- init-client
  [& params]
  (mk-client #(apply mk-mock-beanstalk params) 1 JOB-TTR "infosquito"))


(deftest test-mk-client
  (let [ctor   (fn [])
        client (mk-client ctor 1 JOB-TTR "infosquito")]
    (is (= ctor (:ctor client)))
    (is (= 1 (:conn-tries client)))
    (is (= JOB-TTR (:job-ttr client)))
    (is (= "infosquito" (:tube client)))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server
  (let [client (init-client)]
    (with-server client
      (is (not= nil @(:beanstalk client))))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server-bad-connection
   (let [client (init-client (atom (close default-state)))
         thrown (ss/try+
                  (with-server client)
                  false
                  (catch [:type :connection] {:keys []}
                    true))]
     (is thrown)))


(deftest test-put
  (testing "normal behavior"
    (let [state   (atom default-state)
          client  (init-client state)
          payload "payload"]
      (with-server client (put client payload))
      (is (= [payload JOB-TTR] 
             (map (peek-ready @state "infosquito") [:payload :ttr])))))
  (testing "normal behavior with multibyte characters"
    (let [state   (atom default-state)
          client  (init-client state)
          payload (str "payload" \u00A0)]
      (with-server client (put client payload))
      (is (= [payload JOB-TTR] 
             (map (peek-ready @state "infosquito") [:payload :ttr])))))
  (testing "out of memory"
    (let [state   (atom (set-oom default-state true))
          client  (init-client state)
          thrown? (ss/try+
                    (with-server client (put client "payload"))
                    false
                    (catch [:type :beanstalkd-oom] {} true))]
      (is thrown?)))
  (testing "draining" 
    (let [state   (atom (drain default-state))
          client  (init-client state)
          thrown? (ss/try+
                    (with-server client (put client "payload"))
                    false
                    (catch [:type :beanstalkd-draining] {} true))]
      (is thrown?)))
  (testing "buried"
    (let [state   (atom (set-bury default-state true))
          client  (init-client state)
          thrown? (ss/try+
                    (with-server client (put client "payload"))
                    false
                    (catch [:type :beanstalkd-oom] {} true))]
      (is thrown?))))


(deftest test-reserve
  (let [payload        "payload"
        [state job-id] (init-tubes default-state payload)
        client         (init-client (atom state))]
    (with-server client
      (is (= {:id job-id :payload payload} (reserve client))))))


(deftest test-touch
  (let [payload     "payload"
        [s' job-id] (init-tubes default-state payload)
        state       (-> s'
                      (watch-tube "infosquito")
                      reserve-job
                      first
                      (advance-time 1)
                      atom)
        client     (init-client state)]
    (with-server client (touch client job-id))
    (is (= (:now @state)
           (get-reserve-time @state "infosquito" job-id)))))


(deftest test-release
  (let [[init-state job] (-> default-state
                           (init-tubes "payload")
                           first
                           (watch-tube "infosquito")
                           reserve-job)
        job-id           (:id job)]
    (testing "successful release"
      (let [state  (atom init-state)
            client (init-client state)]
        (with-server client
          (release client job-id))
        (is (= job-id (:id (peek-ready @state "infosquito"))))))
    (testing "buried release"
      (let [client  (init-client (atom (set-bury init-state true)))
            thrown? (ss/try+
                      (with-server client (release client job-id))
                      false
                      (catch [:type :beanstalkd-oom] {} true))]
        (is thrown?)))))


(deftest test-delete
  (let [[init-state job] (-> default-state
                           (init-tubes "payload")
                           first
                           (watch-tube "infosquito")
                           reserve-job)
        job-id           (:id job)]
    (testing "normal behaviour"
      (let [state  (atom init-state)
            client (init-client state)]
        (with-server client (delete client job-id))
        (is (not (jobs? @state)))))
    (testing "bad connection"
      (let [state   (atom init-state)
            client  (init-client state)
            thrown? (ss/try+
                      (with-server client
                        (swap! state close)
                        (delete client job-id))
                      false
                      (catch [:type :connection-closed] {} true))]
        (is thrown?)))))

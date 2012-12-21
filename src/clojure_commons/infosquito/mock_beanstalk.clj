(ns clojure-commons.infosquito.mock-beanstalk
  "This is a mock implementation of a Beanstalk Queue.  It does not support
   priority, delay, time to run (TTR), burying, max-job-size, timeouts for
   reservations, or multiple workers."
  (:require [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]))


(def default-state {:tubes     {"default" []}
                    :using     "default"
                    :watching  #{"default"}
                    :next-id   0
                    :closed?   false
                    :oom?      false
                    :draining? false
                    :bury?     false})


(defn- ensure-tube
  [tubes tube]
  (if (find tubes tube)
    tubes
    (assoc tubes tube [])))


(defn- put-in-tube
  [tubes tube id payload]
  (assoc tubes tube (conj (get tubes tube) {:id id :payload payload})))


(defn- delete-from-tube
  [tubes tube id]
  (let [[pre [subj & post]] (split-with #(not= id (:id %)) (get tubes tube))]
    (if subj
      [(assoc tubes tube (concat pre post)) true]
      [tubes false])))


(defn- validate-open
  [state]
  (when (:closed? state) (ss/throw+ {:type :connection-closed})))


(defn- validate-state
  [state]
  (validate-open state)
  (when (:oom? state) (ss/throw+ {:type :protocol :message "OUT_OF_MEMORY"}))
  (when (:draining? state) (ss/throw+ {:type :protocol :message "DRAINING"})))


(defn- inc-id
  [state]
  (assoc state :next-id (inc (:next-id state))))


(defn- put-task
  [state id payload]
  (assoc state :tubes (put-in-tube (:tubes state) (:using state) id payload)))


(defn- reserve-task
  [state]
  (loop [[hd tl] (seq (:watching state))]
    (when hd
      (let [tube (get (:tubes state) hd)]
        (if-not (empty? tube)
          (first tube)
          (recur tl))))))


(defn- delete-task
  [state id]
  (let [tubes (:tubes state)]
    (loop [[hd tl] (seq (:watching state))]
      (if-not hd (ss/throw+ {:type :not-found}))
      (let [[tubes' found?] (delete-from-tube tubes hd id)]
        (if found?
          (assoc state :tubes tubes')
          (recur tl))))))


(defn- use-tube
  [state tube]
  (assoc state
         :tubes (ensure-tube (:tubes state) tube)
         :using tube))


(defn- watch-tube
  [state tube]
  (assoc state
         :tubes    (ensure-tube (:tubes state) tube)
         :watching (conj (:watching state) tube)))


(defn- ignore-tube
  [state tube]
  (assoc state :watching (disj (:watching state) tube)))


(defrecord ^{:private true} MockBeanstalk [state-ref]
  beanstalk/BeanstalkObject

  (close [_]
    (locking _
      (swap! state-ref #(assoc % :closed? true))
      nil))

  (use [_ tube]
    (locking _
      (validate-open @state-ref)
      (swap! state-ref #(use-tube % tube))))

  (put [_ pri delay ttr bytes data]
    (locking _
      (validate-state @state-ref)
      (let [id  (:next-id @state-ref)]
        (swap! state-ref inc-id)
        (when (:bury? @state-ref)
          (ss/throw+ {:type :protocol :message (str "BURIED " id)}))
        (swap! state-ref #(put-task % id data))
        (.notify _)
        (str "INSERTED " id beanstalk/crlf))))

  (watch [_ tube]
    (locking _
      (validate-open @state-ref)
      (swap! state-ref #(watch-tube % tube))))

  (ignore [_ tube]
    (locking _
      (validate-open @state-ref)
      (swap! state-ref #(ignore-tube % tube))))

  (reserve [_]
    (locking _
      (validate-open @state-ref)
      (if-let [task (reserve-task @state-ref)]
        task
        (do
          (.wait _ 1000)
          (if-let [task (reserve-task @state-ref)]
            task
            (ss/throw+ {:type :deadlock?}))))))

  (reserve-with-timeout [_ timeout]
    (.reserve _))
      
  (delete [_ id]
    (locking _
      (ss/try+
        (validate-open @state-ref)
        (swap! state-ref #(delete-task % id))
        (str "DELETED" beanstalk/crlf)
        (catch [:type :not-found] {:keys []}
          (str "NOT_FOUND" beanstalk/crlf))))))


(defn mk-mock-beanstalk
  [& [state-ref]]
  (when (and state-ref (:closed? @state-ref)) (ss/throw+ (Exception.)))
  (->MockBeanstalk (if state-ref state-ref (atom default-state))))

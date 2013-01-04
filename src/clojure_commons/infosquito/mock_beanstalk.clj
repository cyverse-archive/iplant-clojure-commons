(ns clojure-commons.infosquito.mock-beanstalk
  "This is a mock implementation of a Beanstalk Queue.  It does not support priority, delay, burying, 
   max-job-size, timeouts for reservations, or multiple workers."
  (:require [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]))


; JOB

(defn mk-job
  [id ttr payload]
  {:id      id
   :ttr     ttr
   :payload payload})


(defn identifies?
  [job id]
  (= id (:id job)))


; RESERVATION

(defn mk-reservation
  [job reserve-time]
  {:job job :reserve-time reserve-time})


(defn expired?
  [reservation epoch]
  (> epoch 
     (+ (:reserve-time reservation) (:ttr (:job reservation)))))


(defn for-job?
  [reservation job-id]
  (identifies? (:job reservation) job-id))


(defn renew
  [reservation renewal-time]
  (assoc reservation :reserve-time renewal-time))

  
; TUBE

(defn- mk-tube 
  []
  {:ready '() :reserved #{}})


(defn- tube-ready?
  [tube]
  (not (empty? (:ready tube))))

  
(defn- reserved-in-tube?
  [tube job-id]
  (boolean (some #(for-job? % job-id) (:reserved tube))))


(defn- find-reservation
  [tube job-id]
  (first (drop-while #(not (for-job? % job-id)) 
                     (:reserved tube))))


(defn- put-in-tube
  [tube job]
  (assoc tube :ready (concat (:ready tube) [job])))
  

(defn- reserve-in-tube
  [tube reserve-time]
  (let [ready (:ready tube)]
    (if (empty? ready)
      [tube nil]
      (let [job   (first ready)
            tube' (assoc tube 
                         :ready    (rest ready)
                         :reserved (conj (:reserved tube) (mk-reservation job reserve-time)))]
         [tube' job]))))
  

(defn- touch-in-tube
  [tube job-id touch-time]
  (if (reserved-in-tube? tube job-id)
    (let [reservation (find-reservation tube job-id)
          reserved    (conj (disj (:reserved tube) reservation) (renew reservation touch-time))]
      (assoc tube :reserved reserved))
    tube))
  
    
(defn- release-job
  [tube job-id]
  (if (reserved-in-tube? tube job-id)
    (let [reservation (find-reservation tube job-id)]
      (assoc tube
             :ready    (cons (:job reservation) (:ready tube))
             :reserved (disj (:reserved tube) reservation)))
    tube))
  

(defn- release-expired-jobs
  [tube epoch]
  (let [reserved             (:reserved tube)
        expired-reservations (filter #(expired? % epoch) reserved)]
    (assoc tube 
           :ready    (concat (map :job expired-reservations) (:ready tube))
           :reserved (apply disj reserved expired-reservations))))
  
 
(defn- delete-from-tube
  [tube job-id]
  (assoc tube :reserved (disj (:reserved tube) (find-reservation tube job-id))))
  

; BEANSTALK STATE

(def ^{:private false} default-state {:tubes     {"default" (mk-tube)}
                                      :using     "default"
                                      :watching  #{"default"}
                                      :next-id   0
                                      :now       0
                                      :closed?   false
                                      :oom?      false
                                      :draining? false
                                      :bury?     false})


(defn- validate-open
  [state]
  (when (:closed? state) (ss/throw+ {:type :connection-closed})))


(defn- validate-state
  [state]
  (validate-open state)
  (when (:oom? state) (ss/throw+ {:type :protocol :message "OUT_OF_MEMORY"}))
  (when (:draining? state) (ss/throw+ {:type :protocol :message "DRAINING"})))


(defn- get-tube
  [state tube-name]
  (get (:tubes state) tube-name))


(defn- update-tube
  [state tube-name tube]
  (assoc state :tubes (assoc (:tubes state) tube-name tube)))


(defn- ensure-tube
  [state tube-name]
  (if (contains? (:tubes state) tube-name)
    state
    (update-tube state tube-name (mk-tube))))


(defn- update-tubes
  "update is a function that accepts a name-tube pair"
  [state update]
  (let [tubes (:tubes state)]
    (assoc state 
           :tubes (zipmap (keys tubes) 
                          (map #(update (key %) (val %)) tubes)))))


(defn- ready?
  [state]
  (some #(tube-ready? (get-tube state %)) (:watching state)))
  

(defn- reserved?
  [state job-id]
  (some #(reserved-in-tube? (get-tube state %) job-id) (:watching state)))


(defn- inc-id
  [state]
  (assoc state :next-id (inc (:next-id state))))


(defn- advance-time
  [state delta-time]
  (let [now (+ (:now state) delta-time)]
    (update-tubes (assoc state :now now) #(release-expired-jobs %2 now))))


(defn- put-job
  [state job-id ttr payload]
  (let [tube (:using state)]
    (update-tube state tube (put-in-tube (get-tube state tube) (mk-job job-id ttr payload)))))


(defn- reserve-job
  [state]
  (loop [[tube-name rem-tubes] (seq (:watching state))]
    (if-not tube-name
      [state nil]
      (let [tube (get-tube state tube-name)]
        (if (tube-ready? tube)
          (let [[tube' job] (reserve-in-tube tube (:now state))]
            [(update-tube state name tube') job])
          (recur rem-tubes))))))


(defn- delete-job
  [state job-id]
  (when-not (reserved? state job-id) (ss/throw+ {:type :not-found}))
  (update-tubes state 
                (fn [name tube] (if (contains? (:watching state) name) 
                                  (delete-from-tube tube job-id)
                                  tube))))


(defn- touch-job
  [state job-id]
  (when-not (reserved? state job-id) (ss/throw+ {:type :not-found}))
  (update-tubes state
                (fn [name tube] (if (contains? (:watching state) name)
                                  (touch-in-tube tube job-id (:now state))
                                  tube))))
  
  
(defn- use-tube
  [state tube]
  (assoc (ensure-tube state tube) :using tube))


(defn- watch-tube
  [state tube]
  (let [state' (ensure-tube state tube)]
    (assoc state' :watching (conj (:watching state') tube))))


(defn- ignore-tube
  [state tube]
  (assoc state :watching (disj (:watching state) tube)))


(defn- reserve!
  [state-ref]
  (let [[state' job] (reserve-job @state-ref)]
    (reset! state-ref state')
    job))


(defn advance-time!
  [state-ref delta-time] 
  (swap! @state-ref #(advance-time % delta-time)))
  
  
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

  (put [_ pri delay-time ttr bytes data]
    (locking _
      (validate-state @state-ref)
      (let [id  (:next-id @state-ref)]
        (swap! state-ref inc-id)
        (when (:bury? @state-ref) (ss/throw+ {:type :protocol :message (str "BURIED " id)}))
        (swap! state-ref #(put-job % id ttr data))
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
      (if-let [job (reserve! state-ref)]
        job
        (do
          (.wait _ 1000)
          (if-let [job (reserve! state-ref)]
            job
            (ss/throw+ {:type :deadlock?}))))))

  (reserve-with-timeout [_ timeout]
    (.reserve _))
      
  (delete [_ id]
    (locking _
      (ss/try+
        (validate-open @state-ref)
        (swap! state-ref #(delete-job % id))
        (str "DELETED" beanstalk/crlf)
        (catch [:type :not-found] {:keys []}
          (str "NOT_FOUND" beanstalk/crlf)))))
  
  (touch [_ id]
    (locking _
      (ss/try+
        (validate-open @state-ref)
        (swap! state-ref #(touch-job % id))
        (str "TOUCHED" beanstalk/crlf)
        (catch [:type :not-found] {}
          (str "NOT_FOUND" beanstalk/crlf))))))


(defn mk-mock-beanstalk
  [& [state-ref]]
  (when (and state-ref (:closed? @state-ref)) (ss/throw+ (Exception.)))
  (->MockBeanstalk (if state-ref state-ref (atom default-state))))

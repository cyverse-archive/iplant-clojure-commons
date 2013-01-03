(ns clojure-commons.infosquito.work-queue
  "This namespace wraps the beanstalk clojure client providing the error handling policies.

   All jobs should be idempotent.

   It attempts to remain true to the beanstalkd operations.  Use put to insert a new job.  To remove 
   a job, first reserve it.  When the job is completed, delete it.

   All public functions that talk with the assigned beanstalkd, required a client constructed by the 
   mk-client function, and should be called inside of the with-server function.

   All exceptions use the slingshot extensions."
  (:require [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]))


(defn- nop
  [& args])


(defn- perform-op-once
  [client beanstalk-op & {:keys [oom-handler
                                 drain-handler
                                 bury-handler
                                 deadline-handler]
                          :or   {oom-handler      nop
                                 drain-handler    nop
                                 buried-handler   nop
                                 deadline-handler nop}}]
  (letfn [(get-buried-id [error-msg] ((re-find #"BURIED ([0-9]+)" error-msg) 1))]
    (ss/try+
      (beanstalk-op @(:beanstalk client))
      (catch [:type :expected_crlf] {:keys []} (ss/throw+ {:type :internal-error}))
      (catch [:type :not_found] {keys []})
      (catch [:type :protocol] {:keys [message]}
        (condp #(re-find (re-pattern %1) %2) message
          "OUT_OF_MEMORY"   (oom-handler)
          "INTERNAL_ERROR"  (ss/throw+ {:type :internal-error})
          "DRAINING"        (drain-handler)
          "BAD_FORMAT"      (ss/throw+ {:type :internal-error})
          "BURIED"          (bury-handler (get-buried-id message))
          "JOB_TOO_BIG"     (ss/throw+ {:type :internal-error})
          "DEADLINE_SOON"   (deadline-handler)
          "UNKNOWN_COMMAND" (ss/throw+ {:type :internal-error})
          (ss/throw+ {:type :unknown-error}))))))


;; This is not part of the API
(defn close!
  [client]
  (let [queue-ref (:beanstalk client)]
    (ss/try+
      (when @queue-ref
        (swap! queue-ref beanstalk/close))
      (catch Exception e
        (reset! queue-ref nil)
        (log/warn "Failed to close client connection to beanstalkd: " e)))))


;; This is not part of the API
(defn connect!
  [client]
  (let [queue-ref (:beanstalk client)]
    (loop [attempt-num 1]
      (ss/try+
        (let [tube (:tube client)]
          (reset! queue-ref ((:ctor client)))
          (perform-op-once client #(beanstalk/watch % tube))
          (perform-op-once client #(beanstalk/ignore % "default"))
          (perform-op-once client #(beanstalk/use % tube)))
        (catch Exception e
          (close! client)))
      (when-not @queue-ref
        (if (> attempt-num (:conn-tries client))
          (ss/throw+ {:type :connection :msg "failed to connect to beanstalkd"})
          (recur (inc attempt-num)))))))


(defn- perform-op
  [client beanstalk-op & handlers]
  (letfn [(perform-once [] (apply perform-op-once client beanstalk-op handlers))]
    (ss/try+
      (perform-once)
      (catch [:type :unknown-error] {:keys []}
        (connect! client)
        (perform-once)))))


(defn- has-server?
  [client]
  (not= nil @(:beanstalk client)))
  

(defn mk-client
  "Constructs the client that manages communications the the assigned beanstalkd server.

   Parameters:
     beanstalk-ctor - This is the constructor for the underlying BeanstalkObject.
     connect-tries - This is the number of times the client will attempt to connect to beanstalkd 
       before giving up.
     job-ttr - This is the number of seconds the client will have to perform a job while reserved.
     tube - The name of the tube to use.

   Returns:
     It returns a beanstalk client"
  [beanstalk-ctor connect-tries job-ttr tube]
  {:ctor       beanstalk-ctor
   :conn-tries connect-tries
   :job-ttr    job-ttr
   :tube       tube
   :beanstalk  (atom nil)})


(defmacro with-server
  "This macro connects to a server and executes a sequence of functions.
   Finally, it closes the connection.

   Parameters:
     client - The beanstalk client object
     ops - The functions to execute with an open server connection.

   Throws:
     All execeptions thrown by the ops pass through.
     :connection - This is thrown if it fails to connect to the server."
  [client & ops]
  `(ss/try+
    (connect! ~client)
    (do ~@ops)
    (finally
      (close! ~client))))


(defn delete
  "Removes a job from beanstalkd.

   Parameters:
     client - the beanstalkd client
     job-id - the identifier for the job being removed

   Preconditions:
     The client is connected to a beanstalkd server.

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic internal to the work queue.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [client job-id]
  (assert (has-server? client))
  (perform-op client #(beanstalk/delete % job-id))
  nil)


(defn put
  "Posts a job to beanstalkd.

   Parameters:
     client - the beanstalkd client
     job-str - The serialized job to post

   Preconditions:
     The client is connected to a beanstalkd server.

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error internal to the work 
       queue.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory.
     :beanstalkd-draining - This is thrown if beanstalkd is draining and not accepting new jobs."
  [client job-str]
  (assert (has-server? client))
  (letfn [(put' [beanstalk] (beanstalk/put beanstalk 0 0 (:job-ttr client) (count job-str) job-str))]
    (perform-op client put'
      :oom-handler   #(ss/throw+ {:type :beanstalkd-oom})
      :drain-handler #(ss/throw+ {:type :beanstalkd-draining})
      :bury-handler  (fn [id]
                       (delete client id)
                       (ss/throw+ {:type :beanstalkd-oom})))
    nil))
  

(defn release
  "Releases an unfinished job back to beanstalkd.

   Parameters:
     client - the beanstalk client
     job-id - the identifier for the job being released

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic internal to the work queue.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [client job-id]
  (assert (has-server? client))
  (perform-op client beanstalk/release job-id 0 0)
  nil)


(defn reserve
  "Reserves a job in beanstalkd

   Parameters:
     client - the beanstalkd client

   Returns:
     It returns the job map.  This map has two elements.  :id holds the job identifier and :payload 
     holds the serialized job.

   Preconditions:
     The client is connected to a beanstalkd server.

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error internal to the work 
       queue.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory."
  [client]
  (assert (has-server? client))
  (perform-op client beanstalk/reserve :oom-handler #(ss/throw+ {:type :beanstalkd-oom})))


(defn touch
  "Tells beanstalkd that a job has not been finished.  This renews the reservation.

   Parameters:
     client - the beanstalk client
     job-id - the identifier for the job whose reservation is to be renewed

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic internal to the work queue.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [client job-id]
  (assert (has-server? client))
  (perform-op client beanstalk/touch job-id)
  nil)

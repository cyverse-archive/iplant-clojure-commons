(ns clojure-commons.infosquito
  "This namespace contains a protocol and some types that can will be used by infosquito to
   interact with a work queue."
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.basic :as lb]))

(defprotocol QueueClient
  (publish [this payload]
    "Publishes a message to the queue.")
  (subscribe [this handler]
    "Registers a handler to process messages from the queue."))

(deftype RabbitmqClient [conn channel exchange queue]

  Closeable
  (close [this]
    (rmq/close channel)
    (rmq/close conn))

  QueueClient
  (publish [this payload]
    (lb/publish channel exchange queue payload))

  (subscribe [this handler]
    (lc/subscribe channel queue
                  (fn [ch {:keys [headers delivery-tag]} ^bytes payload]
                    (handler queue (String. payload "UTF-8"))
                    (lb/ack ch delivery-tag))
                  :auto-ack false)))
;; RabbitmqClient

(defn rabbitmq-connection-settings
  "Returns a connection settings map that can be used to establish a connection to RabbitMQ."
  [host port user pass]
  {:host     host
   :port     port
   :username user
   :password pass})

(defn rabbitmq-client
  [connection-settings exchange queue]
  (let [conn    (rmq/connect connection-settings)
        channel (lch/open conn)]
    (le/declare channel exchange "direct" :durable true)
    (lq/declare channel queue :durable true :auto-delete false)
    (RabbitmqClient. conn channel exchange queue)))

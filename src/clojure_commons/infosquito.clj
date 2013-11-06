(ns clojure-commons.infosquito
  "This namespace provides some types that can serve as wrappers for clients of various work
   queues. These types will all implement a single protocol and will thus serve as an abstraction
   layer that allows us to easily switch to different work queues if necessary.

   Creating a new client should automatically establish a connection to the server. In general,
   this means that a factory function should be used to generate each type of queue client. The
   factory function will establish the connection, pass it to the constructor of the appropriate
   QueueClient implementation, and return the newly created QueueClient implementation.

   Each QueueClient implementation must also implement java.io.Closeable. This allows the
   implementation to be used within a with-open macro to ensure that the connection to the server
   is closed cleanly.")

(defprotocol QueueClient
  "A client to some sort of work queue."

  (publish [this payload]
    "Posts a message to the queue.")

  (subscribe [this handler]
    "Subscribes to the queue. The handler will be called every time a message is retrieved
     from the queue. This method blocks the calling thread, so it should normally be called
     from within a dedicated thread."))

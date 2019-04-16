(ns metabase.query-processor.middleware.async
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase.async
             [semaphore-channel :as semaphore-channel]
             [util :as async.u]]
            [metabase.models.setting :refer [defsetting]]
            [metabase.util :as u]
            [metabase.util.i18n :refer [trs]]
            [schema.core :as s])
  (:import clojure.core.async.impl.channels.ManyToManyChannel
           java.util.concurrent.TimeoutException))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                wait-for-permit                                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(defsetting max-simultaneous-queries-per-db
  (trs "Maximum number of simultaneous queries to allow per connected Database.")
  :type    :integer
  :default 15)

(defonce ^:private db-semaphore-channels (atom {}))

(s/defn ^:private fetch-db-semaphore-channel :- ManyToManyChannel
  "Fetch the counting semaphore channel for a Database, creating it if not already created."
  [database-or-id]
  (let [id (u/get-id database-or-id)]
    (or
     ;; channel already exists
     (@db-semaphore-channels id)
     ;; channel does not exist, Create a channel and stick it in the atom
     (let [ch     (semaphore-channel/semaphore-channel (max-simultaneous-queries-per-db))
           new-ch ((swap! db-semaphore-channels update id #(or % ch)) id)]
       ;; ok, if the value swapped into the atom was a different channel (another thread beat us to it) then close our
       ;; newly created channel
       (when-not (= ch new-ch)
         (a/close! ch))
       ;; return the newly created channel
       new-ch))))

(defn wait-for-permit
  "Middleware that asynchronously waits for a per-DB permit before running a query. By default DBs are limited to
  running 15 queries at once (the default size of JDBC connection pools); any queries past this limit will be parked
  until a slot is free."
  [qp]
  (fn [{database-id :database, :as query} respond raise canceled-chan]
    (let [semaphore-chan (fetch-db-semaphore-channel database-id)
          result-chan    (semaphore-channel/do-after-receiving-permit semaphore-chan
                           qp query respond raise canceled-chan)]
      (a/go
        (when (a/<! canceled-chan)
          (a/close! result-chan)))
      result-chan)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  async->sync                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn async->sync
  "Async-style (4-arg) middleware that wraps the synchronous (1-arg) portion of the QP middleware."
  [qp]
  (fn [query respond raise canceled-chan]
    (if (a/poll! canceled-chan)
      (log/debug (trs "Request already canceled, will not run synchronous QP code."))
      (try
        (respond (qp query))
        (catch Throwable e
          (raise e))))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  async-setup                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

;; NOCOMMIT
;; Make this something higher when not running tests.
(def ^:private qp-timeout-ms
  10000)

(defn async-setup
  "Middleware that creates the output/canceled channels for the asynchronous (4-arg) QP middleware and runs it."
  [qp]
  (fn [{:keys [async?], :as query}]
    (let [out-chan      (a/promise-chan)
          canceled-chan (async.u/promise-canceled-chan out-chan)
          respond       (fn [result]
                          (a/>!! out-chan result)
                          (a/close! out-chan))
          raise         (fn [e]
                          (log/warn e (trs "Unhandled exception, exepected `catch-exceptions` middleware to handle it"))
                          (respond e))]
      (try
        (qp query respond raise canceled-chan)
        (catch Throwable e
          (raise e)))
      (if async?
        out-chan
        ;; TODO - should there be a max timeout for `out-chan` to return results?
        (let [[result port] (a/alts!! [(a/timeout qp-timeout-ms) out-chan])]
          (cond
            (not= port out-chan)
            (throw (TimeoutException. (str (trs "Query timed out after {0} milliseconds." qp-timeout-ms))))

            (instance? Throwable result)
            (throw result)

            :else
            result))))))

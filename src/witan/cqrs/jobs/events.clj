(ns witan.cqrs.jobs.events
  (:require [clojure.core.async :refer [chan >! <! close! timeout go-loop]]
            [cheshire.core :as json]
            [taoensso.timbre :as timbre]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]
            [onyx.plugin.kafka]
            [witan.cqrs.jobs.shared]))

(def workflow
  [[:event/in-queue       :event/prepare-store]
   [:event/prepare-store  :event/store]
   [:event/in-queue       :event/aggregator]
   [:event/aggregator     :event/store-aggregate]])

(defn build-catalog
  [batch-size batch-timeout]
  [{:onyx/name :event/in-queue
    :onyx/batch-size batch-size
    :onyx/min-peers 1 ;; should be number of partitions
    :onyx/max-peers 1
    :kafka/topic "event"
    :kafka/group-id "onyx-consumer"
    :kafka/zookeeper "127.0.0.1:2181"
    :kafka/deserializer-fn :witan.cqrs.jobs.shared/deserialize-message-json
    :onyx/plugin :onyx.plugin.kafka/read-messages
    :onyx/type :input
    :onyx/medium :kafka
    :kafka/fetch-size 307200
    :kafka/chan-capacity 1000
    :kafka/offset-reset :smallest
    :kafka/force-reset? false
    :kafka/empty-read-back-off 500
    :kafka/commit-interval 500
    :onyx/doc "Reads messages from a Kafka topic"}

   {:onyx/name :event/prepare-store
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "identity"}

   {:onyx/name :event/aggregator
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "identity"}

   {:onyx/name :event/store
    :onyx/plugin :onyx.peer.function/function
    :onyx/fn :clojure.core/identity
    :onyx/type :output
    :onyx/medium :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Identity output"}

   {:onyx/name :event/store-aggregate
    :onyx/plugin :onyx.peer.function/function
    :onyx/fn :clojure.core/identity
    :onyx/type :output
    :onyx/medium :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Identity output"}])

(def lifecycles
  (->> (build-catalog 0 0)
       (map :onyx/name)
       (mapv #(hash-map :lifecycle/task %
                        :lifecycle/calls :witan.cqrs.jobs.shared/log-calls))
       (into [{:lifecycle/task :event/in-queue
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}])) )

(def flow-conditions
  [])

(defn build-job
  [mode batch-size batch-timeout]
  {:catalog (build-catalog batch-size batch-timeout)
   :workflow workflow
   :lifecycles lifecycles
   :task-scheduler :onyx.task-scheduler/balanced
   :flow-conditions flow-conditions})

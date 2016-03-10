(ns witan.cqrs.jobs.commands
  (:require [clojure.core.async :refer [chan >! <! close! timeout go-loop]]
            [taoensso.timbre :as timbre]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]
            [onyx.plugin.kafka]
            [witan.cqrs.jobs.shared]))

(defn coerce [segment]
  (if (contains? segment :command)
    (assoc segment :coerced? true)
    (assoc segment :error "Does not contain a command key")))

(defn process [segment]
  {:message {:event (str "completed-" (:command segment))}})

(def workflow
  [[:command/in-queue     :command/coerce]
   [:command/coerce       :command/store]
   [:command/coerce       :command/process]
   [:command/process      :event/out-queue]])

(defn build-catalog
  [batch-size batch-timeout]
  [{:onyx/name :command/in-queue
    :onyx/batch-size batch-size
    :onyx/min-peers 1 ;; should be number of partitions
    :onyx/max-peers 1
    :kafka/topic "command"
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

   {:onyx/name :command/coerce
    :onyx/fn :witan.cqrs.jobs.commands/coerce
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Validates/coerces the command string"}

   {:onyx/name :command/process
    :onyx/fn :witan.cqrs.jobs.commands/process
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Process the command and converts it to an event"}

   {:onyx/name :event/out-queue
    :onyx/plugin :onyx.plugin.kafka/write-messages
    :onyx/type :output
    :onyx/medium :kafka
    :kafka/topic "event"
    :kafka/zookeeper "127.0.0.1:2181"
    :kafka/serializer-fn :witan.cqrs.jobs.shared/serialize-message-json
    :kafka/request-size 307200
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Identity output"}

   {:onyx/name :command/store
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
       (into [{:lifecycle/task :command/in-queue
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
              {:lifecycle/task :event/out-queue
               :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}])) )

(defn error? [event old-segment new-segment all-new-segments]
  (contains? new-segment :error))

(def constantly-true (constantly true))

(def flow-conditions
  [{:flow/from :command/coerce
    :flow/to [:command/process]
    :flow/predicate [:not :witan.cqrs.jobs.commands/error?]}
   {:flow/from :command/coerce
    :flow/to [:command/store]
    :flow/predicate :witan.cqrs.jobs.commands/constantly-true}])

(defn build-job
  [mode batch-size batch-timeout]
  {:catalog (build-catalog batch-size batch-timeout)
   :workflow workflow
   :lifecycles lifecycles
   :task-scheduler :onyx.task-scheduler/balanced
   :flow-conditions flow-conditions})

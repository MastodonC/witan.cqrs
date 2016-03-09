(ns witan.cqrs.jobs.commands
  (:require [clojure.core.async :refer [chan >! <! close! timeout go-loop]]
            [cheshire.core :as json]
            [taoensso.timbre :as timbre]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]
            [onyx.plugin.kafka]
            [witan.cqrs.jobs.shared]))

(defn deserialize-message-json [bytes]
  (try
    (json/parse-string (String. bytes "UTF-8") true)
    (catch Exception e
      {:error e})))

(defn coerce [segment]
  (if (contains? segment :command)
    (assoc segment :coerced? true)
    (assoc segment :error "Does not contain a command key")))

(defn process [segment]
  (assoc segment :processed? true))

(def workflow
  [[:command/in-queue     :command/coerce]
   [:command/coerce       :command/store]
   [:command/coerce       :command/process]
   [:command/process      :event/out-queue]
   [:event/out-queue      :event/in-queue]
   [:event/in-queue       :event/prepare-store]
   [:event/prepare-store  :event/store]
   [:event/in-queue       :event/aggregator]
   [:event/aggregator     :event/store-aggregate]])

(defn build-catalog
  [batch-size batch-timeout]
  [{:onyx/name :command/in-queue
    :onyx/batch-size batch-size
    :onyx/min-peers 1 ;; should be number of partitions
    :onyx/max-peers 1
    :kafka/topic "command"
    :kafka/group-id "onyx-consumer"
    :kafka/zookeeper "127.0.0.1:2181"
    :kafka/deserializer-fn :witan.cqrs.jobs.commands/deserialize-message-json
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
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Write the event out to Kafka"}

   {:onyx/name :event/in-queue
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "identity"}

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
    :onyx/doc "Identity output"}

   {:onyx/name :command/store
    :onyx/plugin :onyx.peer.function/function
    :onyx/fn :clojure.core/identity
    :onyx/type :output
    :onyx/medium :function
    :onyx/batch-size batch-size
    :onyx/batch-timeout batch-timeout
    :onyx/doc "Identity output"}])

(def logger (agent nil))
(def pad-length 22)
(defn log-batch [event lifecycle]
  (let [task-name (:onyx/name (:onyx.core/task-map event))]
    (doseq [m (map :message (mapcat :leaves (:tree (:onyx.core/results event))))]
      (let [prefix (format (str "> %-" pad-length "s") task-name)]
        (send logger (fn [_] (println prefix " segment: " m))))))
  {})

(def log-calls
  {:lifecycle/after-batch log-batch})

(def lifecycles
  (->> (build-catalog 0 0)
       (map :onyx/name)
       (mapv #(hash-map :lifecycle/task %
                        :lifecycle/calls :witan.cqrs.jobs.shared/log-calls))
       (into [{:lifecycle/task :command/in-queue
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}])) )

(defn command? [event old-segment new-segment all-new-segments]
  (contains? new-segment :command))

(def constantly-true (constantly true))

(def flow-conditions
  [{:flow/from :command/coerce
    :flow/to [:command/process]
    :flow/predicate :witan.cqrs.jobs.commands/command?}
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

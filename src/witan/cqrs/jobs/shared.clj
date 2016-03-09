(ns witan.cqrs.jobs.shared
  (:require [taoensso.timbre :as timbre]))

(def logger (agent nil))

(defn log-batch [event lifecycle]
  (let [task-name (:onyx/name (:onyx.core/task-map event))]
    (doseq [m (map :message (mapcat :leaves (:tree (:onyx.core/results event))))]
      (send logger (fn [_] (timbre/debug task-name " segment SHARED: " m)))))
  {})

(def log-calls
  {:lifecycle/after-batch log-batch})

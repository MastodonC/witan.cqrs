(ns witan.cqrs.jobs.shared
  (:require [taoensso.timbre :as timbre]
            [cheshire.core :as json]))


(defn deserialize-message-json [bytes]
  (let [as-string (String. bytes "UTF-8")]
    (try
      (json/parse-string as-string true)
      (catch Exception e
        {:parse_error e :original as-string}))))

(defn serialize-message-json [segment]
  (.getBytes (json/generate-string segment)))

;;;;;;;

(def logger (agent nil))

(defn log-batch [event lifecycle]
  (let [task-name (:onyx/name (:onyx.core/task-map event))]
    (doseq [m (map :message (mapcat :leaves (:tree (:onyx.core/results event))))]
      (send logger (fn [_] (timbre/debug task-name " segment: " m)))))
  {})

(def log-calls
  {:lifecycle/after-batch log-batch})

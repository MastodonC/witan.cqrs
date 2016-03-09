(ns witan.cqrs.components.onyx-job
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            [cheshire.core :as json]))

(defonce state (atom nil))
(def id (java.util.UUID/randomUUID))
(def batch-size 10)
(def batch-timeout 1000)

(def env-config
  {:zookeeper/address "127.0.0.1:2181"
   :onyx/id id
   :onyx.log/config {}})

(def peer-config
  {:zookeeper/address "127.0.0.1:2181"
   :onyx/id id
   :onyx.log/config {}
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(defn start-onyx-env!
  [n-peers]
  (let [env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        v-peers (onyx.api/start-peers n-peers peer-group)]
    (reset! state {:v-peers v-peers
                   :peer-group peer-group
                   :env env
                   :ref-count 0})))

(defrecord OnyxJob [job name]
  component/Lifecycle
  (start [component]
    (when-not @state
      (start-onyx-env! 10)
      (timbre/info "Onyx environment was started"))
    (swap! state update-in [:ref-count] inc)
    (timbre/info "Starting Onyx job:" name)
    (let [{:keys [job-id]} (onyx.api/submit-job
                            peer-config
                            job)]
      (assoc component :job-id job-id)))

  (stop [{:keys [job-id] :as component}]
    (when job-id
      (timbre/info "Stopping Onyx job:" name)
      (onyx.api/kill-job peer-config job-id))
    (when @state
      (swap! state update-in [:ref-count] dec)
      (when (-> @state :ref-count zero?)
        (timbre/info "Shutting down Onyx environment" )
        (let [{:keys [v-peers peer-group env]} @state]
          (doseq [v-peer v-peers]
            (onyx.api/shutdown-peer v-peer))
          (onyx.api/shutdown-peer-group peer-group)
          (onyx.api/shutdown-env env)
          (reset! state nil))))
    (dissoc component :job-id)))

(defn new-onyx-job [mode job-sym]
  (map->OnyxJob {:job ((resolve job-sym) mode batch-size batch-timeout)
                 :name (str job-sym)}))

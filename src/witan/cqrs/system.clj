(ns witan.cqrs.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]
            ;;
            [witan.cqrs.jobs.commands]
            ;;
            [witan.cqrs.components.onyx-job :refer [new-onyx-job]]))

(defn new-system
  []
  (let [mode :dev]
    ;;
    (timbre/set-level! :debug)
    ;;
    (component/system-map
     :onyx-commands (new-onyx-job mode 'witan.cqrs.jobs.commands/build-job))))

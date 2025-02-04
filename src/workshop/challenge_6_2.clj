(ns workshop.challenge-6-2
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :bucket-page-views]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :bucket-page-views
       :onyx/plugin :onyx.peer.function/function
       :onyx/fn :clojure.core/identity
       :onyx/type :output
       :onyx/medium :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Identity function, used for windowing segments unchanged."}]))

;;; Lifecycles ;;;

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;; <<< BEGIN FILL ME IN PART 1 >>>

(def windows
  [{:window/id :collect-segments
    :window/task :bucket-page-views
    :window/type :sliding
    :window/aggregation [:onyx.windowing.aggregation/average :bytes-sent]
    :window/window-key :event-time
    :window/range [1 :hour]
    :window/slide [30 :minutes]
    :window/doc "Averages the byte values over 1 hour sliding windows, sliding every 30 minutes."}])

;; <<< END FILL ME IN PART 1 >>>

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/id :sync
    :trigger/on :onyx.triggers/segment
    :trigger/fire-all-extents? true
    :trigger/threshold [10 :elements]
    :trigger/sync ::deliver-promise!
    :trigger/doc "Fires against all extents every 10 segments processed."}])

(def fired-window-state (atom {}))

(defn deliver-promise! [event window {:keys [trigger/window-id] :as trigger} {:keys [lower-bound upper-bound] :as state-event} state]
  ;; <<< BEGIN FILL ME IN PART 2 >>>
  (let [lower (java.util.Date. lower-bound)
        upper (java.util.Date. upper-bound)]
    (swap! fired-window-state assoc [lower upper] state))
  ;; <<< END FILL ME IN PART 2 >>>
  )

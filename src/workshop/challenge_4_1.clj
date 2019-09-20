(ns workshop.challenge-4-1
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times-three]
   [:times-three :write-segments]])

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

      {:onyx/name :times-three
       :onyx/fn :workshop.challenge-4-1/times-three
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by 3"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn times-three [segment]
  (update-in segment [:n] (partial * 3)))

;;; Lifecycles ;;;

(def logger (agent nil))

;; <<< BEGIN FILL ME IN FOR log-segments >>>
;; ;; by me
;; (defn log-task-start [event lifecycle]
;;   (send logger (fn [_] (println "Starting task: " (:onyx.core/task event))))
;;   {})

;; ;; by me
;; ;; Log when the task stops. Return no new values for the event map
;; (defn log-task-stop [event lifecycle]
;;   (send logger (fn [_] (println "Stopping task: " (:onyx.core/task event))))
;;   {})

;; https://github.com/onyx-platform/learn-onyx/blob/answers/src/workshop/challenge_4_1.clj#L53
(defn log-segments [event lifecycle]
  (doseq [m (:onyx.core/batch event)]
    (send logger (fn [_] (println m))))
  {})
;; <<< END FILL ME IN >>>

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

;; <<< BEGIN FILL ME IN FOR logger-lifecycle calls >>>
;; ;; by me
;; (def logger-lifecycle
;;   {:lifecycle/before-task-start log-task-start
;;    :lifecycle/after-task-stop log-task-stop})

;; https://github.com/onyx-platform/learn-onyx/blob/answers/src/workshop/challenge_4_1.clj#L53
(def logger-lifecycle
  {:lifecycle/after-batch log-segments})

;; ;; by me
;; (def writer-lifecycle
;;   {:lifecycle/before-task-start inject-writer-ch})

;; from https://github.com/onyx-platform/learn-onyx/blob/answers/src/workshop/challenge_4_1.clj#L53
(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})
;; <<< END FILL ME IN >>>

(defn build-lifecycles []
  [;; <<< BEGIN FILL ME IN FOR :times-three >>>
   {:lifecycle/task :times-three
    :lifecycle/calls :workshop.challenge-4-1/logger-lifecycle
    :onyx/doc "Logs segments as they're processed"}
   ;; <<< END FILL ME IN >>>

   {:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-4-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

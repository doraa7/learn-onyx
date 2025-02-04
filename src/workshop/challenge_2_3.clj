(ns workshop.challenge-2-3
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :write-segments]])

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

      ;; <<< BEGIN FILL ME IN >>>
      ;; by me
      ;; {:onyx/name :identity
      ;;  :onyx/type :function
      ;;  :onyx/fn :workshop.challenge-2-3/identity
      ;;  :onyx/batch-size batch-size
      ;;  :onyx/batch-timeout batch-timeout
      ;;  :onyx/group-by-key :user-id
      ;;  :onyx/min-peers 2
      ;;  :onyx/flux-policy :continue
      ;;  :onyx/doc "Multiplies :n in the segment by 3"}

      {:onyx/name :identity
       :onyx/fn :clojure.core/identity
       :onyx/type :function
       :onyx/group-by-key :user-id
       :onyx/flux-policy :kill
       :onyx/min-peers 2
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "A simple identity function"}

      ;; <<< END FILL ME IN >>>

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;
;; this is by me
;; but not required
(defn identity [segment]
  (update-in segment [:n] identity))

;;; Lifecycles ;;;

;; Serialize print statements to avoid garbled stdout.
(def printer (agent nil))

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(defn echo-segments [event lifecycle]
  (send printer
        (fn [_]
          (doseq [segment (:onyx.core/batch event)]
            (println (format "Peer %s saw segment %s"
                             (:onyx.core/id event)
                             segment)))))
  {})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def identity-lifecycle
  {:lifecycle/after-batch echo-segments})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-2-3/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :identity
    :lifecycle/calls :workshop.challenge-2-3/identity-lifecycle
    :onyx/doc "Lifecycle for logging segments"}])

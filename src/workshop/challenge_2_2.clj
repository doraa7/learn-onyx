(ns workshop.challenge-2-2
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times]
   [:times :plus]
   [:plus :write-segments]])

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
      {:onyx/name :times
       :onyx/type :function
       :onyx/fn :workshop.challenge-2-2/times
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :my/mult-param 3
       :onyx/params [:my/mult-param]
       :onyx/doc "Multiplies :n in the segment by a value provided via the catalog :onyx/params entry"}

      {:onyx/name :plus
       :onyx/type :function
       :onyx/fn :workshop.challenge-2-2/plus
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :my/sum-param 50
       :onyx/params [:my/sum-param]
       :onyx/doc "Adds fifty to :n in the segment by a value provided via the catalog :onyx/params entry"}
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

(defn times [k segment]
  (update-in segment [:n] (partial * k)))

(defn plus [k segment]
  (update-in segment [:n] (partial + k)))

;;; Lifecycles ;;;

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-2-2/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

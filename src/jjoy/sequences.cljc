(ns jjoy.sequences
  (:refer-clojure :exclude [map pmap conj])
  (:require [clojure.core :as clojure]
            [jjoy.core :as jjoy]))

(defn conj [{:keys [stack] :as thread}]
  (let [[item container stack] (jjoy/consume-stack 2 stack)]
    (assoc thread :stack (clojure/conj stack (clojure/conj container item)))))

(defn map-step [{:keys [stack queue] :as thread}]
  (let [[acc quot xs stack] (jjoy/consume-stack 3 stack)]
    (if (seq xs)
      (let [i (peek xs)
            xs (pop xs)]
        (-> thread
            (assoc :stack (clojure/conj stack xs quot acc i))
            (assoc :queue (into (clojure/conj queue 'jjoy.sequences/map-step 'jjoy.sequences/conj) (reverse quot)))))
      (assoc thread :stack (clojure/conj stack (into (list) (reverse acc)))))))

(defn map [thread]
  (-> thread
      (update :stack clojure/conj [])
      (update :queue clojure/conj 'map-step)))

(defn pmap [{:keys [stack] :as thread}]
  (let [[quot stack] (jjoy/consume-stack 1 stack)
        quot (clojure/conj '(jjoy.kernel/swap jjoy.sequences/conj :spawn) quot)]
    (-> thread
        (assoc :stack (clojure/conj stack quot []))
        (update :queue clojure/conj 'jjoy.sequences/map-step [] '(:consume) 'jjoy.sequences/map-step))))

(defn each [{:keys [stack] :as thread}]
  (let [[quot xs stack] (jjoy/consume-stack 2 stack)]
    (if (seq xs)
      (let [i (peek xs)
            xs (pop xs)]
        (-> thread
            (assoc :stack (clojure/conj stack xs quot))
            (update :queue clojure/conj 'jjoy.sequences/each 'jjoy.kernel/drop :spawn (clojure/conj quot i))))
      (assoc thread :stack stack))))

(defn vocabulary []
  {'map {:jjoy/impl #'map}
   'pmap {:jjoy/impl #'pmap}
   'each {:jjoy/impl #'each}
   'conj {:jjoy/impl #'conj}
   'map-step {:jjoy/impl #'map-step
              :jjoy/private true}})

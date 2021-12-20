(ns jjoy.kernel
  (:refer-clojure :exclude [drop])
  (:require [jjoy.core :as jjoy]))

(defn if* [{:keys [stack] :as thread}]
  (let [[else-q then-q condition stack] (jjoy/consume-stack 3 stack)]
    (-> thread
        (assoc :stack stack)
        (update :queue into (reverse (if condition then-q else-q))))))

(defn swap [{:keys [stack] :as thread}]
  (let [[x1 x2 stack] (jjoy/consume-stack 2 stack)]
    (assoc thread :stack (conj stack x1 x2))))

(defn drop [thread]
  (update thread :stack pop))

(defn vocabulary []
  {'if {:jjoy/impl #'if*}
   'swap {:jjoy/impl #'swap}
   'drop {:jjoy/impl #'drop}})

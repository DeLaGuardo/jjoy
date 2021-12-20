(ns jjoy.shuffle
  (:refer-clojure :exclude [shuffle compile])
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [jjoy.core :as jjoy]))

(s/def :lhs/form (s/and (s/+ :lhs/el)))
(s/def :lhs/el (s/or :binding :common/binding :nested :lhs/nested))
(s/def :lhs/nested (s/spec (s/cat :head (s/* :lhs/el) :rest (s/? :common/rest-binding))))

(s/def :rhs/form (s/* :rhs/el))
(s/def :rhs/el (s/or :binding :common/binding :nested :rhs/nested :rest :common/rest-binding))
(s/def :rhs/nested (s/spec (s/+ :rhs/el)))

(s/def :common/binding (every-pred simple-symbol? #(not (string/starts-with? (name %) ".."))))

(s/def :common/rest-binding (every-pred simple-symbol? #(string/starts-with? (name %) "..")))

(s/def :shuffle/form (s/cat :lsh :lhs/form :rhs :rhs/form))

(declare compile-lhs)

(defmulti compile-lhs-el first)

(defmethod compile-lhs-el :binding [[_ binding]]
  (fn [{:keys [queue bindings]}]
    (if-let [el (peek queue)]
      {:queue (pop queue)
       :bindings (assoc bindings binding el)}
      (throw (ex-info "Stack is empty" {})))))

(defmethod compile-lhs-el :nested [[_ {:keys [head rest]}]]
  (let [head-f (compile-lhs head)]
    (fn [{:keys [queue bindings]}]
      (if-let [el (peek queue)]
        (if (sequential? el)
          (let [{queue' :queue
                 bindings' :bindings} (head-f el)]
            {:queue (pop queue)
             :bindings (merge bindings
                              bindings'
                              (when (some? rest)
                                {rest queue'}))})
          (throw (ex-info "TODO better error message" {})))
        (throw (ex-info "Stack is empty" {}))))))

(defn compile-lhs [lhs]
  (reduce
   (fn [f lhs-el]
     (comp (compile-lhs-el lhs-el) f))
   (fn [queue]
     {:bindings {}
      :queue queue})
   lhs))

(declare compile-rhs)

(defmulti compile-rhs-el first)

(defmethod compile-rhs-el :binding [[_ binding]]
  (fn [{:keys [bindings] :as res}]
    (if (contains? bindings binding)
      (update res :queue conj (bindings binding))
      (throw (ex-info "TODO better error" {})))))

(defmethod compile-rhs-el :nested [[_ form]]
  (let [nested-f (compile-rhs form)]
    (fn [res]
      (update res :queue conj (nested-f (dissoc res :queue))))))

(defmethod compile-rhs-el :rest [[_ rest-binding]]
  (fn [{:keys [bindings] :as res}]
    (if (contains? bindings rest-binding)
      (update res :queue #(reduce conj % (reverse (bindings rest-binding))))
      (throw (ex-info "TODO better error" {})))))

(defn compile-rhs [rhs]
  (reduce
   (fn [f rhs-el]
     (comp f (compile-rhs-el rhs-el)))
   (fn [{:keys [queue]}] queue)
   rhs))

(defn compile [lhs rhs]
  (comp (compile-rhs (s/conform :rhs/form rhs)) (compile-lhs (s/conform :lhs/form lhs))))

(defn shuffle [k]
  (fn [{:keys [stack] :as thread}]
    (let [[after-form before-form stack] (jjoy/consume-stack 2 stack)
          f (compile before-form after-form)]
      (-> thread
          (assoc :stack stack)
          (update k f)))))

(def shuffle-stack (shuffle :stack))

(def shuffle-queue (shuffle :queue))

(defn vocabulary []
  {'shuffle-stack {:jjoy/impl #'shuffle-stack}
   'shuffle-queue {:jjoy/impl #'shuffle-queue}})

(comment

  (s/conform :lhs/form '((a b ..c))) ;; => [[:nested {:head [[:binding a] [:binding b]], :rest ..c}]]
  (s/conform :lhs/form '((ax bz & c))) ;; => [[:nested {:head [[:binding a] [:binding b]], :rest {:amp &, :binding c}}]]

  ((compile '(a) '(a a a)) '(1 2 3 4 5))

  ((compile '(a b) '(b a)) '(1 2 3 4 5))

  (let [f (compile '((((user) ..b) ..c) d) '(user ..b ..c d))
        g (fn [[[[[a] & b] & c] & d]]
            (concat [a] b c d))]
    (time
     (dotimes [_ 100000]
       (f '((((1) 2 3) 4 5) 6 7))))
    (time
     (dotimes [_ 100000]
       (g '((((1) 2 3) 4 5) 6 7))))
    (list (f '((((1) 2 3) 4 5) 6 7))
          (g '((((1) 2 3) 4 5) 6 7))))

  (let [f (compile '(a b) '(b a))
        g (fn [[a b & c]]
            (cons b (cons a c)))]
    (time (dotimes [_ 100000]
            (f '(1 2 3 4))))
    (time (dotimes [_ 100000]
            (g '(1 2 3 4))))
    (list (f '(1 2 3 4))
          (g '(1 2 3 4))))

  )

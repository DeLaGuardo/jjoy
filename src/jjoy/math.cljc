(ns jjoy.math
  (:require [jjoy.core :as jjoy]))

(defmacro binary-op [f]
  `(fn [{stack# :stack :as thread#}]
     (let [[y# x# stack#] (jjoy/consume-stack 2 stack#)]
       (assoc thread# :stack (conj stack# (~f x# y#))))))

(defn vocabulary []
  {'* {:jjoy/impl (binary-op *)}
   '+ {:jjoy/impl (binary-op +)}
   '- {:jjoy/impl (binary-op -)}
   '/ {:jjoy/impl (binary-op /)}
   '> {:jjoy/impl (binary-op >)}
   '< {:jjoy/impl (binary-op <)}
   '<= {:jjoy/impl (binary-op <=)}
   '>= {:jjoy/impl (binary-op >=)}})

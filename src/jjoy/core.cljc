(ns jjoy.core
  (:refer-clojure :exclude [compile]))

(defmulti compile :compiler)

(defmethod compile :jjoy [{:keys [quot] :as definition}]
  (let [quot (reverse quot)
        f #(update % :queue into quot)]
    (assoc definition :jjoy/impl f)))

(defn exec [thread word]
  (let [{:keys [stack queue effects]} (word (select-keys thread [:stack :queue]))]
    (-> thread
        (assoc :stack stack)
        (assoc :queue queue)
        (cond-> (seq effects) (update :effects concat effects)))))

(defn panic! [state thread-id reason]
  (assoc state :panicking {:thread-id thread-id
                           :reason reason}))

(defn qualify-vocab [vocab ns]
  (reduce (fn [acc [k v]]
            (conj acc [(symbol (name ns) (name k)) v]))
          vocab vocab))

(defn- unwind-vocabulary [state vocab-ns]
  (let [vocab-f (requiring-resolve (symbol (name vocab-ns) "vocabulary"))]
    (-> state
        (update :vocabulary merge (qualify-vocab (vocab-f) vocab-ns))
        (update :using conj vocab-ns))))

(defn tick [{:keys [panicking vocabulary prev-thread-ids next-thread-id thread-id threads] :as state}]
  ;; (prn "---TICK" (-> state (dissoc :vocabulary) (update :threads select-keys [thread-id])))
  (if (and thread-id (not panicking))
    (recur
     (if-let [thread (threads thread-id)]
       (try
         (let [{:keys [stack queue]} thread]
           (if (seq queue)
             (let [term (peek queue)
                   state' (update-in state [:threads thread-id :queue] pop)]
               (case term

                 :uses (let [queue (get-in state' [:threads thread-id :queue])
                             nss (peek queue)
                             queue (pop queue)]
                         (reduce unwind-vocabulary
                                 (assoc-in state' [:threads thread-id :queue] queue)
                                 nss))

                 :word (let [queue (get-in state' [:threads thread-id :queue])
                             name (peek queue)
                             queue (pop queue)
                             definition (peek queue)
                             queue (pop queue)]
                         (-> state'
                             (update :vocabulary conj [name (assoc (compile definition) :userspace true)])
                             (assoc-in [:threads thread-id :queue] queue)))

                 :park (cond-> state'
                         true (dissoc :thread-id)
                         (seq prev-thread-ids) (-> (assoc :thread-id (peek prev-thread-ids))
                                                   (update :prev-thread-ids pop)))

                 :unpark (if (= thread-id 0)
                           (let [thread-to-unpark (peek stack)
                                 stack' (pop stack)
                                 queue-to-unpark-with (peek stack')]
                             (-> state'
                                 (assoc-in [:threads thread-id :stack] (pop stack'))
                                 (assoc :thread-id thread-to-unpark)
                                 (update-in [:threads thread-to-unpark :queue] into (reverse queue-to-unpark-with))))
                           (panic! state thread-id "Trying to unpark from child thread"))

                 :spawn (-> state'
                            (assoc :next-thread-id (inc next-thread-id))
                            (update :prev-thread-ids conj thread-id)
                            (assoc :thread-id next-thread-id)
                            (update-in [:threads thread-id :stack] (comp #(conj % next-thread-id) pop))
                            (assoc-in [:threads next-thread-id] {:id next-thread-id
                                                                 :stack ()
                                                                 :queue (peek stack)}))

                 :consume (let [consume-from (peek stack)
                                {consuming-thread :consumer
                                 consume-from-queue :queue} (threads consume-from)]
                            (cond
                              (and consuming-thread (not= consuming-thread thread-id))
                              (panic! state thread-id "The thread to be consumed from is delivering results to another thread.")

                              (empty? consume-from-queue)
                              (-> state'
                                  (update-in [:threads thread-id :stack] pop)
                                  (assoc :thread-id consume-from)
                                  (assoc-in [:threads consume-from :consumer] thread-id))

                              :else
                              (-> state'
                                  (update-in [:threads thread-id :stack] pop)
                                  ((if (seq consume-from-queue) dissoc assoc) :thread-id)
                                  (assoc-in [:threads consume-from :consumer] thread-id))))

                 (if-let [{word :jjoy/impl} (vocabulary term)]
                   (try
                     (update-in state' [:threads thread-id] exec word)
                     (catch Throwable e
                       (panic! state thread-id e)))
                   (update-in state' [:threads thread-id :stack] conj term))))

             ;; queue is empty
             (let [consumer (-> thread-id threads :consumer)]
               (cond-> state
                 true (dissoc :thread-id)
                 (seq prev-thread-ids) (-> (assoc :thread-id (peek prev-thread-ids))
                                           (update :prev-thread-ids pop))
                 (and consumer (not= thread-id consumer)) (-> (assoc :thread-id consumer)
                                                              (update-in [:threads consumer :queue] into stack)
                                                              (update :threads dissoc thread-id))))))
         (catch Throwable e
           (panic! state thread-id e)))
       (dissoc state :thread-id)))
    ;; GC ?
    state))

(defn consume-stack [n stack]
  (loop [n n acc [] stack stack]
    (if (not= n 0)
      (recur (dec n) (conj acc (peek stack)) (pop stack))
      (conj acc stack))))

(defn- init-state [program]
  {:thread-id 0
   :threads {0 {:id 0
                :queue program
                :stack ()}}
   :next-thread-id 1
   :using #{}})

(defn- do-effects [{:keys [threads] :as state} handler]
  (reduce
   (fn [state [thread-id {:keys [effects]}]]
     (doseq [effect effects]
       (handler effect))
     (update-in state [:threads thread-id] dissoc :effects))
   state
   threads))

(defn- continue [state effect-handler]
  (-> state
      (tick)
      (do-effects effect-handler)))

(defn- serialize-word [vocab [name {:keys [userspace] :as definition}]]
  (if userspace
    (conj vocab [name (dissoc definition :jjoy/impl)])
    vocab))

(defn serialize [state]
  (-> state
      (update :vocabulary #(reduce serialize-word {} %))))

(defn- deserialize-word [vocab [name definition]]
  (conj vocab [name (compile definition)]))

(defn deserialize [{:keys [using] :as state}]
  (reduce unwind-vocabulary (update state :vocabulary #(reduce deserialize-word {} %)) using))

(defn run
  ([program] (run program (constantly nil)))
  ([program effect-handler]
   (continue (init-state program) effect-handler)))

(defn unpark
  ([state thread-id queue] (unpark state thread-id queue (constantly nil)))
  ([state thread-id queue effect-handler]
   (if (contains? (:threads state) thread-id)
     (continue
      (-> state
          (assoc :thread-id 0)
          (update-in [:threads 0 :queue] into [:unpark thread-id queue]))
      effect-handler)
     (panic! state 0 "Unparking non existing thread"))))

(defn result [state]
  (get-in state [:threads 0 :stack]))

(comment

  ;; Small program to add a number to items in the list
  ;; number to add comes from the user
  (-> '(#_ ""
           :uses (jjoy.kernel jjoy.sequences jjoy.math)
           (1 2 3)
           (:park +) pmap)
      (run)
      ;; Partialy computed program with four running threads. Main thread is waiting until the user send a number to continue. Serializable to edn.
      (unpark 1 '(42))
      ;; One thread finished, yay! Stil serializable to edn can be transfered to another host to continue execution
      (unpark 3 '(40))
      ;; Finishing up last thread introduced by pmap
      (unpark 2 '(41))
      (result)) ;; => ((43 43 43))

  (->
   '(#_ "Custom words"
        :uses (jjoy.math)
        :word inc {:compiler :jjoy :quot (1 +)}
        :word dec {:compiler :jjoy :quot (1 -)}

        1 inc dec)
   (run)
   (result))

  (-> '(#_ "Serialization/deserialization test"
           :uses (jjoy.kernel jjoy.sequences jjoy.math)
           :word inc {:compiler :jjoy :quot (1 +)}
           :word dec {:compiler :jjoy :quot (1 -)}

           :park (inc :park dec +) pmap)
      (run)
      (unpark 0 '((1 2 3)))
      (serialize)                       ;; transferable object
      (deserialize)                     ;; another vm might deserialize and continue evaluation
      (unpark 3 '(40))
      (unpark 1 '(10))
      (unpark 2 '(30))
      (serialize))

  (-> '(#_ "Panic!"
           :uses (jjoy.math)
           1 0 /)
      (run))

  (-> '(#_ "Pmap"
           :uses (jjoy.kernel jjoy.math jjoy.sequences)

           (1 2 3) (1 +) pmap)
      (run))

  (-> '(#_ "Example uses"
           :uses (jjoy.kernel jjoy.sequences)

           false (42) (43) if)
      (run)
      (serialize)
      (deserialize))

  (-> '(#_ "Each example"
           :uses (jjoy.kernel jjoy.math jjoy.sequences)
           (1 2 3) (1 +) each)
      (run))

  )

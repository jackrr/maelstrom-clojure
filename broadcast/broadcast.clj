#!/usr/bin/env bb

(ns maelstrom-clojure.broadcast
  (:gen-class)
  (:require
   [cheshire.core :as json]
   [clojure.core.async :as async]))

(def children (atom []))
(def ledger (atom {}))

(def node-id (atom ""))
(def next-message-id (atom 0))
(def id-idx (atom 0))
(def unacked (atom {}))

;;;;;;;;;;;;;;;;;;; Util functions ;;;;;;;;;;;;;;;;;;;

;;;;;; Input pre-processing functions ;;;;;;

(defn- process-stdin
  "Read lines from the stdin and calls the handler"
  [handler]
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (handler line)))


(defn- parse-json
  "Parse the received input as json"
  [input]
  (try
    (json/parse-string input true)
    (catch Exception e
      nil)))


;;;;;; Output Generating functions ;;;;;;

(defn- generate-json
  "Generate json string from input"
  [input]
  (when input
    (json/generate-string input)))


(defn- printerr
  "Print the received input to stderr"
  [input]
  (binding [*out* *err*]
    (println input)))

(defn- printout
  "Print the received input to stdout"
  [input]
  (when input
    (println input)))

(defn gen-uuid []
  (str @node-id "-" (swap! id-idx inc)))

(defn send
  [target-node-id body]
  (-> {:src @node-id
       :dest target-node-id
       :body body}
      generate-json
      printout))

(def flush-interval-ms 10)
(defn flush-unacked
  "Infinite loop -- sleep 10 ms and resend all in unacked"
  []
  (while true
    (when (< 0 (count @unacked))
      (doseq [p (vals @unacked)]
        (send (:peer p) (:payload p))))
    (Thread/sleep (long flush-interval-ms))))

(defn unacked-key
  [peer nonce]
  (str peer nonce))

(defn ack
  "Handle replication ack message"
  [peer nonce]
  (swap! unacked #(dissoc % (unacked-key peer nonce))))

(defn record-and-propagate
  "Record broadcast message and distribute to peers that haven't seen it"
  [val seen nonce]
  (swap! ledger #(assoc % nonce val)) ;; use nonce as key to prevent dupes
  (doseq [peer (clojure.set/difference @children (set seen))]
    (let [payload {:type "replicate"
                   :val val
                   :nonce nonce 
                   :seen (conj seen @node-id)}]
      (swap! unacked #(assoc %
                             (unacked-key peer nonce)
                             {:payload payload
                              :peer peer}))
      (send peer payload))))

(defn- process-request
  [input]
  (let [body (:body input)
        src (:src input)
        reply-body {:msg_id (swap! next-message-id inc)
                    :in_reply_to (:msg_id body)}
        reply (partial send src)]
    (case (:type body)
      "init"
      (do
        (reset! node-id (:node_id body))
        (reply (assoc reply-body :type "init_ok")))

      "broadcast"
      ;; Accept a broadcast as primary
      (do
        (record-and-propagate (:message body) [] (gen-uuid))
        (reply (assoc reply-body :type "broadcast_ok")))

      "replicate"
      ;; Accept a broadcast replication as a secondary
      (do
        (let [nonce (:nonce body)]
          (record-and-propagate (:val body) (:seen body) nonce)
          (reply (assoc reply-body
                        :type "replicate_ok"
                        :nonce nonce))))

      "replicate_ok"
      ;; Clear message from unacked
      (do
        (ack (:src input) (:nonce body))
        nil)

      "read"
      (reply (assoc reply-body
                    :type "read_ok"
                    :messages (vals @ledger)))
      
      "topology"
      (do
        (reset! children (set (get-in body [:topology (keyword @node-id)])))
        (reply (assoc reply-body
                      :type "topology_ok"))))))

(defn -main
  "It's a server"
  []
  (async/go (flush-unacked))
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))

(-main)

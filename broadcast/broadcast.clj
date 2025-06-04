#!/usr/bin/env bb

(ns maelstrom-clojure.uuids
  (:gen-class)
  (:require
    [cheshire.core :as json]))


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

(def children (atom []))
(def ledger (atom []))

(def node-id (atom ""))
(def next-message-id (atom 0))
(def id-idx (atom 0))

(defn gen-uuid []
  (str @node-id (swap! id-idx inc)))

(defn reply
  [input body]
  {:src @node-id
   :dest (:src input)
   :body body})

(defn send
  "Send a message out of request/response cycle"
  [target-node-id body]
  (-> {:src @node-id
       :dest target-node-id
       :body body}
      generate-json
      printout))

(defn send-replicate
  "Send replicate message and record that we are awaiting ack"
  [peer-id body]
  (send peer-id body))

;; TODO: periodically flush unacked
(def unacked (atom {}))

(def flush-interval-ms 10)
(defn flush-unacked
  "Infinite loop -- sleep 10 ms and resend all in unacked"
  []
  (while true
    (printerr "Flushing acks")
    (if (< 0 (count unacked))
      (doseq [{peer-id :peer-id payload :payload} (vals unacked)]
        (send peer-id payload))
      (printerr (str "No unacks: " unacked)))
    (Thread/sleep flush-interval-ms)))

(defn record-and-propagate
  "Record broadcast message and distribute to peers that haven't seen it"
  [val seen nonce]
  (swap! ledger #(conj % val))
  (doseq [peer-id (clojure.set/difference @children (set seen))]
    (let [payload {:type "replicate"
                   :val val
                   :nonce nonce 
                   :seen (conj seen @node-id)}]
      (swap! unacked #(assoc %
                             (str peer-id nonce)
                             {:payload payload
                                :peer peer-id}))
      (send peer-id payload))))

(defn- process-request
  [input]
  (let [body (:body input)
        reply-body {:msg_id (swap! next-message-id inc)
                    :in_reply_to (:msg_id body)}
        reply (partial reply input)]
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
        (record-and-propagate (:val body) (:seen body) (:nonce body))
        (reply (assoc reply-body :type "replicate_ok")))

      "replicate_ok"
      ;; Clear message from unacked
      (do
        (swap! unacked #(dissoc % (str (:src body) (:nonce body))))
        (printerr "Received ack"))

      "read"
      (reply (assoc reply-body
                    :type "read_ok"
                    :messages @ledger))
      
      "topology"
      (do
        (reset! children (set (get-in body [:topology (keyword @node-id)])))
        (printerr @children)
        (reply (assoc reply-body
                      :type "topology_ok"))))))

(defn -main
  "It's a server"
  []
  (let [flusher (future (flush-unacked))]
    (process-stdin (comp printout
                         generate-json
                         process-request
                         parse-json))
    (printerr "CANCELLING AHHHHH!!!!!!!!!!")
    (future-cancel flusher)))


(-main)

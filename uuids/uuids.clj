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
  (json/generate-string input))


(defn- printerr
  "Print the received input to stderr"
  [input]
  (binding [*out* *err*]
    (println input)))


(defn- printout
  "Print the received input to stdout"
  [input]
  (println input))


(defn reply
  ([src dest body]
   {:src src
    :dest dest
    :body body}))


(def node-id (atom ""))
(def next-message-id (atom 0))
(def id-idx (atom 0))

(defn gen-uuid []
  (str @node-id (swap! id-idx inc)))

(defn- process-request
  [input]
  (let [body (:body input)
        reply-body {:msg_id (swap! next-message-id inc)
                :in_reply_to (:msg_id body)}]
    (case (:type body)
      "init"
      (do
        (reset! node-id (:node_id body))
        (reply @node-id
               (:src input)
               (assoc reply-body :type "init_ok")))
      "generate"
      (reply @node-id
             (:src input)
             (assoc reply-body
                    :id (gen-uuid)
                    :type "generate_ok"))
      "echo"
      (reply @node-id
             (:src input)
             (assoc reply-body
                    :type "echo_ok"
                    :echo (:echo body))))))

(defn -main
  "It's a server"
  []
  (process-stdin (comp printout
                       generate-json
                       process-request
                       parse-json)))


(-main)

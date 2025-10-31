(ns maelstrom.datomic.maelstrom.datomic
  (:require [cheshire.core :as json])
  (:gen-class))

(defn reply [req body]
  (let [msg (assoc body
                   :src (:dest req)
                   :dest (:src req)
                   :in_reply_to (:msg_id (:body req)))]
    (println (json/generate-string msg))))

(defn handle-message [req]
  (let [{:keys [type]} (:body req)]
    (case type
      ; TODO: handle init {:type "init_ok", :src nil, :dest nil, :in_reply_to nil, :body {}} ??
      "init" (reply nil {:type "init_ok"})
      "echo" (reply req {:type "echo_ok"})
      (reply req {:type "error" :code 10 :text (str "unknown type " type)}))))

(defn -main [& _]
  (loop []
    (when-let [line (read-line)]
      (let [req (json/parse-string line true)]
        (handle-message req)
        (recur)))))

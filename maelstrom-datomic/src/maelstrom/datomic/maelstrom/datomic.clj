(ns maelstrom.datomic.maelstrom.datomic
  (:require [cheshire.core :as json])
  (:gen-class))

(defn reply [req body]
  (let [msg {:src  (:dest req)
             :dest (:src req)
             :body (assoc body
                          :in_reply_to (:msg_id (:body req)))}]
    (println (json/generate-string msg))))

(defn handle-message [req]
  (let [{:keys [type msg_id]} (:body req)]
    (case type
      "init"
      (let [resp {:src  (:dest req)
                  :dest (:src req)
                  :body {:type "init_ok" :in_reply_to msg_id}}]
        (println (json/generate-string resp)))

      "echo"
      (reply req {:type "echo_ok" :echo (:echo (:body req))})

      (reply req {:type "error" :code 10 :text (str "unknown type " type)}))))

(defn -main [& _]
  (loop []
    (when-let [line (read-line)]
      (let [req (json/parse-string line true)]
        (handle-message req)
        (recur)))))


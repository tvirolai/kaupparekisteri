(ns kaupparekisteri.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clj-time.local :as l]))

#_(def url1 "http://avoindata.prh.fi/bis/v1?totalResults=false&maxResults=10&resultsFrom=10&companyRegistrationFrom=1800-01-01")

(def url "http://avoindata.prh.fi/bis/v1?totalResults=false&maxResults=1000&companyRegistrationFrom=1800-01-01&resultsFrom=")

(defn query [url]
  (-> url client/get :body json/read-str clojure.walk/keywordize-keys))

(defn extract-names [res]
  (->> res :results (map :name)))

(defn fetch-all []
  (let [conn (mg/connect)
        db (mg/get-db conn "kaupparekisteri")]
    (loop [page 0]
      (let [data (-> (str url page) query :results)]
        (do
          (mc/insert-batch db "data" data)
          (println data)
          (Thread/sleep 10000))
        (recur (+ page 1000))))))

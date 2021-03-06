(ns kaupparekisteri.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.string :as s]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clj-time.local :as l]))

(def time-formatter
  (f/formatter "yyyy-MM-dd"))

(defn previousdate
 "Returns a date object for a date [days] prior to present."
 [days]
  (-> days t/days t/ago))

(def end-date
  (t/date-time 1900 1 1))

(defn set-id [item]
  (clojure.set/rename-keys item {:businessId :_id}))

(defn parse-response [res]
  (->> res :body json/read-str clojure.walk/keywordize-keys :results (map set-id)))

(defn parse-query
  "Takes two integers, which denote the time range for the query as days from the present.
  Returns a parsed query string, which fetches results from a five-day range."
  [start daycount]
  (let [formatter (partial f/unparse time-formatter)
        startdate (->> start previousdate formatter)
        lastdate (->> (- start daycount) previousdate formatter)]
    (str "http://avoindata.prh.fi/bis/v1?totalResults=true&maxResults=1000&companyRegistrationFrom=" startdate
         "&companyRegistrationTo=" lastdate)))

(defn fetch-all [start]
  (let [conn (mg/connect)
        db (mg/get-db conn "kauppa")]
    (loop [begin start]
      (let [date (previousdate begin)
            data (client/get (parse-query begin 8))
            resultset (parse-response data)
            dates (re-seq #"\d{4}-\d{2}-\d{2}" (parse-query begin))]
        (if (t/after? end-date date)
          "Done."
          (do
            (mc/insert-batch db "data" resultset)
            (println (str "Fetched " (count resultset) " results (" (first dates) " - " (last dates)", that is " begin " days...)"))
            (Thread/sleep 2000)
            (recur (+ begin 8))))))))

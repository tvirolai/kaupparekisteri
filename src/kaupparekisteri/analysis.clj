(ns kaupparekisteri.analysis
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as s]
            [incanter.core :as i]
            [incanter.charts :as c]
            [incanter.io :as iio]))

(defn extract-names! [inputfile outputfile]
  (loop [lines (line-seq (clojure.java.io/reader inputfile))]
    (if (empty? lines)
      (println "Done.")
      (let [record (clojure.walk/keywordize-keys (json/read-str (first lines)))
            line (str \" (s/join "\",\"" (vals record)) \")]
        (do
          (spit outputfile (str line "\n") :append true)
          (recur (rest lines)))))))

(def vowels '("a" "e" "i" "o" "u" "y"))

(def consonants '("b" "c" "d" "f" "g" "h" "j" "l" "m" "n" "p" "q" "r" "s" "t" "v" "w" "x" "z"))

(defn word-follows-formula? [word]
  (if (not= 6 (count word))
    false
    (let [characters (partition 2 (s/lower-case word))
          c (map (comp str first) characters)
          v (map (comp str last) characters)]
      (and
        (every? (set vowels) v)
        (every? (set consonants) c)))))

(defn name-follows-formula? [entry]
  (let [words (s/split entry #" ")]
    (boolean (some true? (map word-follows-formula? words)))))

(defn name-follows-strictly-formula? [entry]
  (let [words (s/split entry #" ")]
    (and 
      (boolean (word-follows-formula? (first words))) 
      (< (count words) 4))))

(defmulti load-data identity)

(defmethod load-data :basic [_]
  (-> "./data/yritykset.csv" (iio/read-dataset :header true)))

(defmethod load-data :names [_]
  (->> (load-data :basic)
       (i/$ :nimi)
       (filter #(> (count %) 1))))

(defmethod load-data :fittingnames [_]
  (->> (load-data :names)
       (filter name-follows-formula?)))

(defmethod load-data :strictlyfittingnames [_]
  (->> (load-data :names)
       (filter name-follows-strictly-formula?)))

(defmethod load-data :onlynames [_]
  (->> (iio/read-dataset "./data/strictly-fitting.csv" :header false)
       (i/$ 0)
       set))

(defn charmatrix-to-freqtable [cmatrix]
  (let [charcounts (map #(frequencies (i/$ % cmatrix)) (range 6))
        totalcount (->> charcounts first vals (reduce +))]
    (map (fn [ccount] (into {} (for [[k v] ccount] [k (/ v totalcount)]))) charcounts)))

(defn names-to-matrix []
  (->> (load-data :onlynames)
       (map (comp seq s/lower-case))
       (i/to-dataset)
       (i/rename-cols {0 :0
                       1 :1
                       2 :2
                       3 :3
                       4 :4
                       5 :5})
       (charmatrix-to-freqtable)
       (map clojure.set/map-invert)))

(defn print-fitting! [output]
  (loop [data (load-data :strictlyfittingnames)]
    (if (empty? data)
      (println "Done.")
      (let [curr (first data)
            words (s/split curr #" ")
            fitting (apply str (filter word-follows-formula? words))]
        (do
          (spit output (str fitting "," curr "\n") :append true)
          (recur (rest data)))))))


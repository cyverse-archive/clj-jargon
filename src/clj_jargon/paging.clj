(ns clj-jargon.paging
  (:import [org.irods.jargon.core.pub.io FileIOOperations]
           [org.irods.jargon.core.pub.io FileIOOperations$SeekWhenceType]))

(def SEEK-CURRENT (FileIOOperations$SeekWhenceType/SEEK_CURRENT))
(def SEEK-START (FileIOOperations$SeekWhenceType/SEEK_START))
(def SEEK-END (FileIOOperations$SeekWhenceType/SEEK_END))

(defn bytes->string
  [buffer]
  (apply str (map char buffer)))

(defn random-access-file
  [cm filepath]
  (.instanceIRODSRandomAccessFile (:fileFactory cm) filepath))

(defn file-length-bytes
  [cm filepath]
  (.length (random-access-file cm filepath)))

(defn read-at-position
  [cm filepath position num-bytes]
  (let [access-file (random-access-file cm filepath)
        file-size   (file-length-bytes cm filepath)
        array-size  (if (< file-size num-bytes) file-size num-bytes)
        buffer      (byte-array array-size)]
    (cond
      (= file-size 0) ""
      (= num-bytes 0) ""
      (< num-bytes 0) ""
      :else
      (do
        (doto access-file
          (.seek position SEEK-CURRENT)
          (.read buffer)
          (.close))
        (String. buffer)))))

(defn overwrite-at-position
  [cm filepath position update]
  (let [access-file  (random-access-file cm filepath)
        update-bytes (.getBytes update)
        read-buffer  (byte-array (count update-bytes))]
    (doto access-file
      (.seek position SEEK-CURRENT)
      (.write update-bytes)
      (.close))
    nil))

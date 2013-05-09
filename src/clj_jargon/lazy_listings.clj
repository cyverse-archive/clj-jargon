(ns clj-jargon.lazy-listings)

(defn- get-next-offset
  [page]
  (let [entry (last page)]
    (if-not (.isLastResult entry)
      (.getCount entry)
      0)))

(defn- lazy-listing
  [get-page]
  (letfn [(get-seq [offset]
            (let [page        (get-page offset)
                  next-offset (get-next-offset page)]
              (if (pos? next-offset)
                (lazy-cat page (get-seq next-offset))
                page)))]
    (get-seq 0)))

(defn list-subdirs-in
  [cm dir-path]
  (lazy-listing #(.listCollectionsUnderPathWithPermissions (:lister cm) dir-path %)))

(defn list-files-in
  [cm dir-path]
  (lazy-listing #(.listDataObjectsUnderPathWithPermissions (:lister cm) dir-path %)))

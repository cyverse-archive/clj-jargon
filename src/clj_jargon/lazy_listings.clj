(ns clj-jargon.lazy-listings
  (:import [org.irods.jargon.core.exception JargonException]
           [org.irods.jargon.core.pub.domain SpecificQueryDefinition]))

(def ^:private queries
  {"ilsLACollections"
   (str "SELECT c.parent_coll_name, c.coll_name, c.create_ts, c.modify_ts, c.coll_id, "
        "c.coll_owner_name, c.coll_owner_zone, c.coll_type, u.user_name, u.zone_name, "
        "a.access_type_id, u.user_id FROM r_coll_main c "
        "JOIN r_objt_access a ON c.coll_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "WHERE c.parent_coll_name = ? "
        "LIMIT ? "
        "OFFSET ?")

   "ilsLADataObjects"
   (str "SELECT s.coll_name, s.data_name, s.create_ts, s.modify_ts, s.data_id, s.data_size, "
        "s.data_repl_num, s.data_owner_name, s.data_owner_zone, u.user_name, u.user_id, "
        "a.access_type_id,  u.user_type_name, u.zone_name FROM "
        "( SELECT c.coll_name, d.data_name, d.create_ts, d.modify_ts, d.data_id, "
        "d.data_repl_num, d.data_size, d.data_owner_name, d.data_owner_zone  FROM r_coll_main c "
        "JOIN r_data_main d ON c.coll_id = d.coll_id  "
        "WHERE c.coll_name = ?  "
        "ORDER BY d.data_name) s "
        "LEFT OUTER JOIN r_objt_access a ON s.data_id = a.object_id "
        "LEFT OUTER JOIN r_user_main u ON a.user_id = u.user_id "
        "LIMIT ? "
        "OFFSET ?")})

(defn- get-query-ao
  "Gets the specific query AO for the current iRODS session."
  [cm]
  (.getSpecificQueryAO (:accessObjectFactory cm) (:irodsAccount cm)))

(defn- define-query
  "Defines a specific query in iRODS. This function ignores exceptions thrown by Jargon because
   Jargon will throw an exception if the query already exists."
  [query-ao [alias query]]
  (try
    (.addSpecificQuery query-ao (SpecificQueryDefinition. alias query))
    (catch JargonException _)))

(defn define-specific-queries
  "Defines the specific queries used for data object and collection listings if they're not
   already defined."
  [irods]
  (dorun (map (partial define-query (get-query-ao irods)) queries)))

(defn- delete-query
  "Deletes a specific query in iRODS."
  [query-ao [alias query]]
  (.removeSpecificQuery query-ao (SpecificQueryDefinition. alias query)))

(defn delete-specific-queries
  "Deletes the specific queries used for data object and collection listings."
  [irods]
  (dorun (map (partial delete-query (get-query-ao irods))  queries)))

(defn- get-next-offset
  "Gets the next offset from a page of results, returning zero if the page is the last page in
   the result set."
  [page]
  (let [entry (last page)]
    (if-not (.isLastResult entry)
      (.getCount entry)
      0)))

(defn- lazy-listing
  "Produces a lazy listing of a set of data objects or collections, given a function that can
   be used to obtain the next page from the result set."
  [get-page]
  (letfn [(get-seq [offset]
            (let [page        (get-page offset)
                  next-offset (get-next-offset page)]
              (if (pos? next-offset)
                (lazy-cat page (get-seq next-offset))
                page)))]
    (get-seq 0)))

(defn list-subdirs-in
  "Returns a lazy listing of the subdirectories in the directory at the given path."
  [cm dir-path]
  (lazy-listing #(.listCollectionsUnderPathWithPermissions (:lister cm) dir-path %)))

(defn list-files-in
  "Returns a lazy listing of the files in the directory at the given path."
  [cm dir-path]
  (lazy-listing #(.listDataObjectsUnderPathWithPermissions (:lister cm) dir-path %)))

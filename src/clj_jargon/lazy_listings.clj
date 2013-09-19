(ns clj-jargon.lazy-listings
  (:require [clj-jargon.spec-query :as sq]
            [clojure-commons.file-utils :as ft]))

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
        "JOIN r_objt_access a ON s.data_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "LIMIT ? "
        "OFFSET ?")

   "IPCUserCollectionPerms"
   (str "SELECT a.access_type_id, u.user_name "
        "FROM r_coll_main c "
        "JOIN r_objt_access a ON c.coll_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "WHERE c.parent_coll_name = ? "
        "AND c.coll_name = ? "
        "LIMIT ? "
        "OFFSET ? ")

   "IPCUserDataObjectPerms"
   "SELECT distinct o.access_type_id, u.user_name 
      FROM r_user_main u,
           r_data_main d,
           r_coll_main c,
           r_tokn_main t,
           r_objt_access o  
     WHERE c.coll_name = ?
       AND d.data_name = ?
       AND c.coll_id = d.coll_id
       AND o.object_id = d.data_id
       AND t.token_namespace = 'access_type'
       AND u.user_id = o.user_id
       AND o.access_type_id = t.token_id
     LIMIT ?
    OFFSET ?"
   
   "IPCDataObjectsAndCollections"
   "WITH user_lookup AS ( SELECT u.user_id as user_id FROM r_user_main u WHERE u.user_name = ?),
         parent AS ( SELECT c.coll_id as coll_id, c.coll_name as coll_name FROM r_coll_main c WHERE c.coll_name = ? )
    SELECT p.full_path, p.create_ts, p.modify_ts, p.access_type_id, p.type
      FROM ( SELECT c.coll_name      as dir_name,
                    d.data_name      as full_path, 
                    d.create_ts      as create_ts, 
                    d.modify_ts      as modify_ts,
                    'dataobject'     as type,
                    a.access_type_id as access_type_id
               FROM r_data_main d
               JOIN r_coll_main c ON c.coll_id = d.coll_id 
               JOIN r_objt_access a ON d.data_id = a.object_id
               JOIN r_user_main u ON a.user_id = u.user_id,
                    user_lookup,
                    parent
              WHERE u.user_id = user_lookup.user_id
                AND c.coll_id = parent.coll_id
              UNION
             SELECT c.parent_coll_name as dir_name,
                    c.coll_name        as full_path, 
                    c.create_ts        as create_ts, 
                    c.modify_ts        as modify_ts,
                    'collection'       as type, 
                    a.access_type_id   as access_type_id
               FROM r_coll_main c 
               JOIN r_objt_access a ON c.coll_id = a.object_id
               JOIN r_user_main u ON a.user_id = u.user_id,
                    user_lookup,
                    parent
              WHERE u.user_id = user_lookup.user_id
                AND c.parent_coll_name = parent.coll_name) AS p
  ORDER BY p.full_path
     LIMIT ?
    OFFSET ?"
   
   "IPCCountDataObjectsAndCollections"
   "WITH user_lookup AS ( SELECT u.user_id as user_id FROM r_user_main u WHERE u.user_name = ?),
         parent AS ( SELECT c.coll_id as coll_id, c.coll_name as coll_name FROM r_coll_main c WHERE c.coll_name = ? )
    SELECT COUNT(p.*)
      FROM ( SELECT c.coll_name      as dir_name,
                    d.data_name      as full_path, 
                    d.create_ts      as create_ts, 
                    d.modify_ts      as modify_ts,
                    'dataobject'     as type,
                    a.access_type_id as access_type_id
               FROM r_data_main d
               JOIN r_coll_main c ON c.coll_id = d.coll_id 
               JOIN r_objt_access a ON d.data_id = a.object_id
               JOIN r_user_main u ON a.user_id = u.user_id,
                    user_lookup,
                    parent
              WHERE u.user_id = user_lookup.user_id
                AND c.coll_id = parent.coll_id
              UNION
             SELECT c.parent_coll_name as dir_name,
                    c.coll_name        as full_path, 
                    c.create_ts        as create_ts, 
                    c.modify_ts        as modify_ts,
                    'collection'       as type, 
                    a.access_type_id   as access_type_id
               FROM r_coll_main c 
               JOIN r_objt_access a ON c.coll_id = a.object_id
               JOIN r_user_main u ON a.user_id = u.user_id,
                    user_lookup,
                    parent
              WHERE u.user_id = user_lookup.user_id
                AND c.parent_coll_name = parent.coll_name) AS p"

   "findQueryByAlias"
   (str "SELECT alias, sqlStr FROM r_specific_query WHERE alias = ?")})

(defn define-specific-queries
  "Defines the specific queries used for data object and collection listings if they're not
   already defined."
  [cm]
  (sq/define-specific-queries cm queries))

(defn delete-specific-queries
  "Deletes the specific queries used for data object and collection listings."
  [cm]
  (sq/delete-specific-queries cm queries))

(defn user-collection-perms
  "Lists the users and their permissions that have access to a collection."
  [cm collection]
  (sq/execute-specific-query
    cm "IPCUserCollectionPerms" 50000 (ft/dirname collection) collection))

(defn user-dataobject-perms
  "Lists the user and their permissions that have access to a dataobject."
  [cm dataobject-path]
  (sq/execute-specific-query 
    cm "IPCUserDataObjectPerms" 50000 (ft/dirname dataobject-path) (ft/basename dataobject-path)))

(defn- get-next-offset
  "Gets the next offset from a page of results, returning zero if the page is the last page in
   the result set."
  [page]
  (let [entry (last page)]
    (if-not (or (empty? page) (.isLastResult entry))
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

(defn paged-list-entries
  "Returns a paged directory listing."
  [cm user dir-path limit offset]
  (sq/paged-query cm "IPCDataObjectsAndCollections" limit offset user dir-path))

(defn count-list-entries
  "Returns the number of entries in a directory listing. Useful for paging."
  [cm user dir-path]
  (-> (sq/get-specific-query-results cm "IPCCountDataObjectsAndCollections" user dir-path)
    (first)
    (first)
    (Integer/parseInt)))


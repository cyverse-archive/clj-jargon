(ns clj-jargon.jargon
  (:use clojure-commons.error-codes
        [slingshot.slingshot :only [try+ throw+]])
  (:require [clojure-commons.file-utils :as ft]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [slingshot.slingshot :as ss])
  (:import [org.irods.jargon.core.exception DataNotFoundException]
           [org.irods.jargon.core.protovalues FilePermissionEnum UserTypeEnum]
           [org.irods.jargon.core.pub.domain
            AvuData
            ObjStat$SpecColType]
           [org.irods.jargon.core.connection IRODSAccount]
           [org.irods.jargon.core.pub IRODSFileSystem]
           [org.irods.jargon.core.pub.io
            IRODSFileReader
            IRODSFileInputStream
            FileIOOperations
            FileIOOperations$SeekWhenceType]
           [org.irods.jargon.core.query
            IRODSGenQuery
            IRODSGenQueryBuilder
            IRODSQueryResultSet
            QueryConditionOperators
            RodsGenQueryEnum
            AVUQueryElement
            AVUQueryElement$AVUQueryPart
            AVUQueryOperatorEnum]
           [org.irods.jargon.datautils.datacache
            DataCacheServiceFactoryImpl]
           [org.irods.jargon.datautils.shoppingcart
            FileShoppingCart
            ShoppingCartEntry
            ShoppingCartServiceImpl]
           [java.io Closeable FileInputStream]
           [org.irods.jargon.ticket
            TicketServiceFactoryImpl
            TicketAdminServiceImpl
            TicketClientSupport]
           [org.irods.jargon.ticket.packinstr
            TicketInp
            TicketCreateModeEnum]))

(declare permissions)

(def read-perm FilePermissionEnum/READ)
(def write-perm FilePermissionEnum/WRITE)
(def own-perm FilePermissionEnum/OWN)
(def none-perm FilePermissionEnum/NONE)

; Debuging code.
(def with-jargon-index (ref 0))
(def ^:dynamic curr-with-jargon-index nil)

(def default-proxy-ctor
  "This is the default constructor for creating an iRODS proxy."
  #(IRODSFileSystem/instance))

(defn init
  "Creates the iRODS configuration map.

    Parameters:
      host - The IP address or FQDN of the iRODS server that will be used.
      port - The IP port the iRODS server listens to.
      username - The iRODS user name of the account that will be used while
        connected to iRODS.
      password - The password of user.
      home - The path to the user's home collection.
      zone - The zone to use
      defaultResource - The default resource to use.
      max-retries - The number of times to retry connecting to the server.  This
        defaults to 0.
      retry-sleep - The number of milliseconds to wait between connection
        retries.  This defaults to 0.
      use-trash - Indicates whether or to put deleted entries in the trash.
        This defaults to false.
      proxy-ctor - This is the constructor to use for creating the iRODS proxy.
        It takes no arguments, and the object its creates must implement have
        the following methods.
        ((close [_])
         (^IRODSAccessObjectFactory getIRODSAccessObjectFactory [_])
         (^IRODSFileFactory getIRODSFileFactory [_ ^IRODSAccount acnt]))
        These must be sematically equivalent to the corresponding methods in
        org.irods.jargon.core.pub.IRODSFileSystem.  This argument defaults to
        default-proxy-ctor.

    Returns:
      A map is returned with the provided parameters names and values forming
      the key-value pairs."
  [host port user pass home zone res
   & {:keys [max-retries retry-sleep use-trash proxy-ctor]
      :or   {max-retries 0
             retry-sleep 0
             use-trash   false
             proxy-ctor  default-proxy-ctor}}]
    {:host            host
     :port            port
     :username        user
     :password        pass
     :home            home
     :zone            zone
     :defaultResource res
     :max-retries     max-retries
     :retry-sleep     retry-sleep
     :use-trash       use-trash
     :proxy-ctor      proxy-ctor})

(defn account
  ([cfg]
    (account cfg (:username cfg) (:password cfg)))
  ([cfg user pass]
    (IRODSAccount. (:host cfg)
                   (Integer/parseInt (:port cfg))
                   user
                   pass
                   (:home cfg)
                   (:zone cfg)
                   (:defaultResource cfg))))

(defn clean-return
  [cm retval]
  (.close (:fileSystem cm))
  retval)

(defn- context-map
  "Throws:
     org.irods.jargon.core.exception.JargonException - This is thrown when if fails to connect to iRODS"
  [cfg]
  (let [acnt        (account cfg)
        file-system ((:proxy-ctor cfg))
        aof         (.getIRODSAccessObjectFactory file-system)]
    (assoc cfg
      :irodsAccount        acnt
      :fileSystem          file-system
      :accessObjectFactory aof
      :collectionAO        (.getCollectionAO aof acnt)
      :dataObjectAO        (.getDataObjectAO aof acnt)
      :userAO              (.getUserAO aof acnt)
      :userGroupAO         (.getUserGroupAO aof acnt)
      :fileFactory         (.getIRODSFileFactory file-system acnt)
      :fileSystemAO        (.getIRODSFileSystemAO aof acnt)
      :lister              (.getCollectionAndDataObjectListAndSearchAO aof
                                                                       acnt)
      :quotaAO             (.getQuotaAO aof acnt)
      :executor            (.getIRODSGenQueryExecutor aof acnt))))

(defn- log-value
  [msg value]
  (log/debug curr-with-jargon-index "-" msg value)
  value)

(defn- get-context
  "Throws:
     org.irods.jargon.core.exception.JargonException - This is thrown when if fails to connect to iRODS"
  [cfg]
  (let [retval {:succeeded true :retval nil :exception nil :retry false}]
    (try
      (log-value "retval:" (assoc retval :retval (context-map cfg)))
      (catch java.net.ConnectException e
        (log/debug curr-with-jargon-index "- caught a ConnectException:" e)
        (log/debug curr-with-jargon-index "- need to retry...")
        (assoc retval :exception e :succeeded false :retry true))
      (catch java.lang.Exception e
        (log/debug curr-with-jargon-index "- got an Exception:" e)
        (log/debug curr-with-jargon-index "- shouldn't retry...")
        (assoc retval :exception e :succeeded false :retry false)))))

(defn create-jargon-context-map
  "Creates a map containing instances of commonly used Jargon objects.

   Throws:
     org.irods.jargon.core.exception.JargonException - This is thrown when if fails to connect to iRODS"
  [cfg]
  (loop [num-tries 0]
    (let [retval (get-context cfg)
          error? (not (:succeeded retval))
          retry? (:retry retval)]
      (cond
        (and error? retry? (< num-tries (:max-retries cfg)))
        (do (Thread/sleep (:retry-sleep cfg))
          (recur (inc num-tries)))

        error?
        (throw (:exception retval))

        :else (:retval retval)))))

(def max-path-length 1067)
(def max-dir-length 640)
(def max-filename-length (- max-path-length max-dir-length))
(def ERR_BAD_DIRNAME_LENGTH "ERR_BAD_DIRNAME_LENGTH")
(def ERR_BAD_BASENAME_LENGTH "ERR_BAD_BASENAME_LENGTH")
(def ERR_BAD_PATH_LENGTH "ERR_BAD_PATH_LENGTH")

(defn validate-full-dirpath
  [full-dirpath]
  (if (> (count full-dirpath) max-dir-length)
    (throw+ {:error_code ERR_BAD_DIRNAME_LENGTH
             :dir-path full-dirpath
             :full-path full-dirpath})))

(defn validate-path-lengths
  [full-path]
  (let [dir-path (ft/dirname full-path)
        file-path (ft/basename full-path)]
    (cond
     (> (count full-path) max-path-length)
     (throw+ {:error_code ERR_BAD_PATH_LENGTH
              :full-path full-path})

     (> (count dir-path) max-dir-length)
     (throw+ {:error_code ERR_BAD_DIRNAME_LENGTH
              :dir-path dir-path
              :full-path full-path})

     (> (count file-path) max-filename-length)
     (throw+ {:error_code ERR_BAD_BASENAME_LENGTH
              :file-path file-path
              :full-path full-path}))))

(defn trash-base-dir
  "Returns the base trash directory either for all users or for a specified user."
  ([cm]
     (ft/path-join "/" (:zone cm) "trash" "home" (:username cm)))
  ([cm user]
     (ft/path-join (trash-base-dir cm) user)))

(defn user-groups
  "Returns a list of group names that the user is in."
  [cm user]
  (for [ug (.findUserGroupsForUser (:userGroupAO cm) user)]
    (.getUserGroupName ug)))

(defn result-row->vec
  [rr]
  (vec (.getColumnsAsList rr)))

(defmacro print-it
  [form]
  `(let [res# ~form]
     (println (str (quote ~form)) "=" res#)
     res#))

(defmacro print-result
  [form]
  `(let [res# ~form]
     (println res#)
     res#))

(defn- escape-gen-query-char
  [c]
  (cond (= c "\\") "\\\\\\\\"
        :else      (str "\\\\" c)))

(defn column-xformer
  [col]
  (cond
   (= (type col) RodsGenQueryEnum)
   (.getName col)

   :else
   (string/replace col #"['\\\\]" escape-gen-query-char)))

(defn gen-query-col-names
  [cols]
  (into-array (mapv column-xformer cols)))

(defn execute-gen-query
  [cm sql cols]
  (.getResults
   (.executeIRODSQueryAndCloseResult
    (:executor cm)
    (IRODSGenQuery/instance
     (String/format sql (gen-query-col-names cols))
     50000)
    0)))

(defn username->id
  [cm user]
  (->> (execute-gen-query cm
        "select %s where %s = '%s'"
        [RodsGenQueryEnum/COL_USER_ID
         RodsGenQueryEnum/COL_USER_NAME
         user])
       (mapv result-row->vec)
       (first)
       (first)))

(defn- collection-perms-rs
  [cm user coll-path]
  (execute-gen-query cm
   "select %s where %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_COLL_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_NAME
    coll-path
    RodsGenQueryEnum/COL_COLL_ACCESS_USER_NAME
    user]))

(defn user-collection-perms
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (->> (conj (set (user-groups cm user)) user)
       (map #(collection-perms-rs cm % coll-path))
       (apply concat)
       (map #(.getColumnsAsList %))
       (map #(FilePermissionEnum/valueOf (Integer/parseInt (first %))))
       (set)))

(defn- dataobject-perms-rs
  [cm user-id data-path]
  (execute-gen-query cm
   "select %s where %s = '%s' and %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_DATA_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_NAME
    (ft/dirname data-path)
    RodsGenQueryEnum/COL_DATA_NAME
    (ft/basename data-path)
    RodsGenQueryEnum/COL_DATA_ACCESS_USER_ID
    user-id]))

(defn user-dataobject-perms
  [cm user data-path]
  (validate-path-lengths data-path)
  (->> (conj (set (user-groups cm user)) user)
       (map (partial username->id cm))
       (map #(dataobject-perms-rs cm % data-path))
       (apply concat)
       (map #(.getColumnsAsList %))
       (map #(FilePermissionEnum/valueOf (Integer/parseInt (first %))))
       (set)))

(defn perm-map-for
  [perms-code]
  (let [perms (FilePermissionEnum/valueOf (Integer/parseInt perms-code))]
    {:read  (contains? #{read-perm write-perm own-perm} perms)
     :write (contains? #{write-perm own-perm} perms)
     :own   (contains? #{own-perm} perms)}))

(defn list-subdirs-rs
  [cm user coll-path]
  (execute-gen-query cm
   "select %s, %s, %s, %s where %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_COLL_NAME
    RodsGenQueryEnum/COL_COLL_CREATE_TIME
    RodsGenQueryEnum/COL_COLL_MODIFY_TIME
    RodsGenQueryEnum/COL_COLL_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_PARENT_NAME
    coll-path
    RodsGenQueryEnum/COL_COLL_ACCESS_USER_NAME
    user]))

(def perm-order-map
  (into {} (map vector [read-perm write-perm own-perm] (range))))

(defn str->perm-const
  [s]
  (FilePermissionEnum/valueOf (Integer/parseInt s)))

(defn format-listing
  [format-fn perm-pos listing]
  (letfn [(select-listing [[_ v]]
            [(apply max-key #(perm-order-map (str->perm-const (nth % perm-pos))) v)])]
    (->> (apply concat listing)
         (map result-row->vec)
         (group-by first)
         (mapcat select-listing)
         (map format-fn))))

(defn format-dir
  [[path create-time mod-time perms]]
  {:date-created  (str (* (Integer/parseInt create-time) 1000))
   :date-modified (str (* (Integer/parseInt mod-time) 1000))
   :file-size     0
   :hasSubDirs    true
   :id            path
   :label         (ft/basename path)
   :permissions   (perm-map-for perms)})

(defn list-subdirs
  [cm user coll-path]
  (sort-by (comp string/upper-case :label)
           (format-listing
            format-dir 3
            (map #(list-subdirs-rs cm % coll-path)
                 (conj (user-groups cm user) user)))))

(defn list-files-in-dir-rs
  [cm user-id coll-path]
  (execute-gen-query cm
   "select %s, %s, %s, %s, %s where %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_DATA_NAME
    RodsGenQueryEnum/COL_D_CREATE_TIME
    RodsGenQueryEnum/COL_D_MODIFY_TIME
    RodsGenQueryEnum/COL_DATA_SIZE
    RodsGenQueryEnum/COL_DATA_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_NAME
    coll-path
    RodsGenQueryEnum/COL_DATA_ACCESS_USER_ID
    user-id]))

(defn format-file
  [coll-path [name create-time mod-time size perms]]
  {:date-created  (str (* (Integer/parseInt create-time) 1000))
   :date-modified (str (* (Integer/parseInt mod-time) 1000))
   :file-size     size
   :permissions   (perm-map-for perms)
   :id            (ft/path-join coll-path name)
   :label         name})

(defn list-files-in-dir
  [cm user coll-path]
  (sort-by (comp string/upper-case :label)
           (format-listing
            (partial format-file coll-path) 4
            (map #(list-files-in-dir-rs cm (username->id cm %) coll-path)
                 (conj (user-groups cm user) user)))))

(defn list-dir-rs
  [cm user coll-path]
  (execute-gen-query cm
   "select %s, %s, %s, %s where %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_COLL_NAME
    RodsGenQueryEnum/COL_COLL_CREATE_TIME
    RodsGenQueryEnum/COL_COLL_MODIFY_TIME
    RodsGenQueryEnum/COL_COLL_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_NAME
    coll-path
    RodsGenQueryEnum/COL_COLL_ACCESS_USER_NAME
    user]))

(defn list-dir
  [cm user coll-path & {:keys [include-files include-subdirs]
                        :or   {include-files   false
                               include-subdirs true}}]
  (let [coll-path (ft/rm-last-slash coll-path)
        listing   (first (map (comp format-dir result-row->vec) (list-dir-rs cm user coll-path)))]
    (when-not (nil? listing)
      (reduce (fn [listing [_ k f]] (assoc listing k (f cm user coll-path)))
              listing
              (filter first
                      [[include-subdirs :folders list-subdirs]
                       [include-files :files list-files-in-dir]])))))

(defn dataobject-perm-map
  "Uses (user-dataobject-perms) to grab the 'raw' permissions for
   the user for the dataobject at data-path, and returns a map with
   the keys :read :write and :own. The values are booleans."
  [cm user data-path]
  (validate-path-lengths data-path)
  (let [perms  (user-dataobject-perms cm user data-path)
        read   (or (contains? perms read-perm)
                   (contains? perms write-perm)
                   (contains? perms own-perm))
        write  (or (contains? perms write-perm)
                   (contains? perms own-perm))
        own    (contains? perms own-perm)]
    {:read  read
     :write write
     :own   own}))

(defn collection-perm-map
  "Uses (user-collection-perms) to grab the 'raw' permissions for
   the user for the collection at coll-path and returns a map with
   the keys :read, :write, and :own. The values are booleans."
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (let [perms  (user-collection-perms cm user coll-path)
        read   (or (contains? perms read-perm)
                   (contains? perms write-perm)
                   (contains? perms own-perm))
        write  (or (contains? perms write-perm)
                   (contains? perms own-perm))
        own    (contains? perms own-perm)]
    {:read  read
     :write write
     :own   own}))

(defn dataobject-perm?
  "Utility function that checks to see of the user has the specified
   permission for data-path."
  [cm username data-path checked-perm]
  (validate-path-lengths data-path)
  (let [perms (user-dataobject-perms cm username data-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn dataobject-readable?
  "Checks to see if the user has read permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (validate-path-lengths data-path)
  (or (dataobject-perm? cm user data-path read-perm)
      (dataobject-perm? cm user data-path write-perm)))

(defn dataobject-writeable?
  "Checks to see if the user has write permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (validate-path-lengths data-path)
  (dataobject-perm? cm user data-path write-perm))

(defn owns-dataobject?
  "Checks to see if the user has ownership permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (validate-path-lengths data-path)
  (dataobject-perm? cm user data-path own-perm))

(defn collection-perm?
  "Utility function that checks to see if the user has the specified
   permission for the collection path."
  [cm username coll-path checked-perm]
  (validate-path-lengths coll-path)
  (let [perms (user-collection-perms cm username coll-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn collection-readable?
  "Checks to see if the user has read permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (or (collection-perm? cm user coll-path read-perm)
      (collection-perm? cm user coll-path write-perm)))

(defn collection-writeable?
  "Checks to see if the suer has write permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (collection-perm? cm user coll-path write-perm))

(defn owns-collection?
  "Checks to see if the user has ownership permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (collection-perm? cm user coll-path own-perm))

(defn file
  [cm path]
  "Returns an instance of IRODSFile representing 'path'. Note that path
    can point to either a file or a directory.

    Parameters:
      path - String containing a path.

    Returns: An instance of IRODSFile representing 'path'."
  (validate-path-lengths path)
  (.instanceIRODSFile (:fileFactory cm) path))

(defn exists?
  [cm path]
  "Returns true if 'path' exists in iRODS and false otherwise.

    Parameters:
      path - String containing a path.

    Returns: true if the path exists in iRODS and false otherwise."
  (validate-path-lengths path)
  (.exists (file cm path)))

(defn paths-exist?
  [cm paths]
  "Returns true if the paths exist in iRODS.

    Parameters:
      paths - A sequence of strings containing paths.

    Returns: Boolean"
  (doseq [p paths] (validate-path-lengths p))
  (zero? (count (filter #(not (exists? cm %)) paths))))

(defn is-file?
  [cm path]
  "Returns true if the path is a file in iRODS, false otherwise."
  (validate-path-lengths path)
  (.isFile (.instanceIRODSFile (:fileFactory cm) path)))

(defn is-dir?
  [cm path]
  "Returns true if the path is a directory in iRODS, false otherwise."
  (validate-path-lengths path)
  (let [ff (:fileFactory cm)
        fixed-path (ft/rm-last-slash path)]
    (.isDirectory (.instanceIRODSFile ff fixed-path))))

(defn is-linked-dir?
  [cm path]
  "Indicates whether or not a directory (collection) is actually a link to a
   directory (linked collection).

   Parameters:
     cm - the context map
     path - the absolute path to the directory to check.

   Returns:
     It returns true if the path points to a linked directory, otherwise it
     returns false."
  (validate-path-lengths path)
  (= ObjStat$SpecColType/LINKED_COLL
     (.. (:fileFactory cm)
       (instanceIRODSFile (ft/rm-last-slash path))
       initializeObjStatForFile
       getSpecColType)))

(defn user-perms->map
  [user-perms-obj]
  (let [enum-val (.getFilePermissionEnum user-perms-obj)]
    {:user (.getUserName user-perms-obj)
     :permissions {:read  (or (= enum-val read-perm) (= enum-val own-perm))
                   :write (or (= enum-val write-perm) (= enum-val own-perm))
                   :own   (= enum-val own-perm)}}))

(defn list-user-perms
  [cm abs-path]
  (let [path' (ft/rm-last-slash abs-path)]
    (validate-path-lengths path')
    (if (is-file? cm path')
      (mapv
        user-perms->map
        (.listPermissionsForDataObject (:dataObjectAO cm) path'))
      (mapv
        user-perms->map
        (.listPermissionsForCollection (:collectionAO cm) path')))))

(defn set-dataobj-perms
  [cm user fpath read? write? own?]
  (validate-path-lengths fpath)

  (let [dataobj (:dataObjectAO cm)
        zone    (:zone cm)]
    (.removeAccessPermissionsForUserInAdminMode dataobj zone fpath user)
    (cond
      own?   (.setAccessPermissionOwnInAdminMode dataobj zone fpath user)
      write? (.setAccessPermissionWriteInAdminMode dataobj zone fpath user)
      read?  (.setAccessPermissionReadInAdminMode dataobj zone fpath user))))

(defn set-coll-perms
  [cm user fpath read? write? own? recursive?]
  (validate-path-lengths fpath)
  (let [coll    (:collectionAO cm)
        zone    (:zone cm)]
    (.removeAccessPermissionForUserAsAdmin coll zone fpath user recursive?)

    (cond
      own?   (.setAccessPermissionOwnAsAdmin coll zone fpath user recursive?)
      write? (.setAccessPermissionWriteAsAdmin coll zone fpath user recursive?)
      read?  (.setAccessPermissionReadAsAdmin coll zone fpath user recursive?))))

(defn set-permissions
  ([cm user fpath read? write? own?]
     (set-permissions cm user fpath read? write? own? false))
  ([cm user fpath read? write? own? recursive?]
     (validate-path-lengths fpath)
     (cond
      (is-file? cm fpath)
      (set-dataobj-perms cm user fpath read? write? own?)

      (is-dir? cm fpath)
      (set-coll-perms cm user fpath read? write? own? recursive?))))

(defn list-paths
  "Returns a list of paths for the entries under the parent path.  This is not
   recursive.  Directories end with /.

   Parameters:
     cm - The context map
     parent-path - The path of the parrent collection (directory).
     :ignore-child-exns - When this flag is provided, child names that are too
       long will not cause an exception to be thrown.  If they are not ignored,
       an exception will be thrown, causing no paths to be listed.  An ignored
       child will be replaced with a nil in the returned list.

   Returns:
     It returns a list path names for the entries under the parent.

   Throws:
     FileNotFoundException - This is thrown if parent-path is not in iRODS.
     See validate-path-lengths for path-related exceptions."
  [cm parent-path & flags]
  (validate-path-lengths parent-path)
  (mapv
    #(try+
       (let [full-path (ft/path-join parent-path %1)]
         (if (is-dir? cm full-path)
           (ft/add-trailing-slash full-path)
           full-path))
       (catch Object _
         (when-not (contains? (set flags) :ignore-child-exns) (throw+))))
    (.getListInDir (:fileSystemAO cm) (file cm parent-path))))

(defn data-object
  [cm path]
  "Returns an instance of DataObject represeting 'path'."
  (validate-path-lengths path)
  (.findByAbsolutePath (:dataObjectAO cm) path))

(defn collection
  [cm path]
  "Returns an instance of Collection (the Jargon version) representing
    a directory in iRODS."
  (validate-path-lengths path)
  (.findByAbsolutePath (:collectionAO cm) (ft/rm-last-slash path)))

(defn lastmod-date
  [cm path]
  "Returns the date that the file/directory was last modified."
  (validate-path-lengths path)
  (cond
    (is-dir? cm path)  (str (long (.getTime (.getModifiedAt (collection cm path)))))
    (is-file? cm path) (str (long (.getTime (.getUpdatedAt (data-object cm path)))))
    :else              nil))

(defn created-date
  [cm path]
  "Returns the date that the file/directory was created."
  (validate-path-lengths path)
  (cond
    (is-dir? cm path)  (str (long (.. (collection cm path) getCreatedAt getTime)))
    (is-file? cm path) (str (long (.. (data-object cm path) getUpdatedAt getTime)))
    :else              nil))

(defn- dir-stat
  [cm path]
  "Returns status information for a directory."
  (validate-path-lengths path)
  (let [coll (collection cm path)]
    {:type     :dir
     :created  (str (long (.. coll getCreatedAt getTime)))
     :modified (str (long (.. coll getModifiedAt getTime)))}))

(defn- file-stat
  [cm path]
  "Returns status information for a file."
  (validate-path-lengths path)
  (let [data-obj (data-object cm path)]
    {:type     :file
     :size     (.getDataSize data-obj)
     :created  (str (long (.. data-obj getUpdatedAt getTime)))
     :modified (str (long (.. data-obj getUpdatedAt getTime)))}))

(defn stat
  [cm path]
  "Returns status information for a path."
  (validate-path-lengths path)
  (cond
   (is-dir? cm path)  (dir-stat cm path)
   (is-file? cm path) (file-stat cm path)
   :else              nil))

(defn file-size
  [cm path]
  "Returns the size of the file in bytes."
  (validate-path-lengths path)
  (.getDataSize (data-object cm path)))

(defn response-map
  [action paths]
  {:action action :paths paths})

(defn user-exists?
  [cm user]
  "Returns true if 'user' exists in iRODS."
  (try
    (do
      (.findByName (:userAO cm) user)
      true)
    (catch java.lang.Exception d false)))

(defn set-owner
  [cm path owner]
  "Sets the owner of 'path' to the username 'owner'.

    Parameters:
      cm - The iRODS context map
      path - The path whose owner is being set.
      owner - The username of the user who will be the owner of 'path'."
  (validate-path-lengths path)
  (cond
   (is-file? cm path)
   (.setAccessPermissionOwn (:dataObjectAO cm) (:zone cm) path owner)

   (is-dir? cm path)
   (.setAccessPermissionOwn (:collectionAO cm) (:zone cm) path owner true)))

(defn set-inherits
  [cm path]
  "Sets the inheritance attribute of a collection to true.

    Parameters:
      cm - The iRODS context map
      path - The path being altered."
  (validate-path-lengths path)
  (if (is-dir? cm path)
    (.setAccessPermissionInherit (:collectionAO cm) (:zone cm) path false)))

(defn permissions-inherited?
  [cm path]
  "Determines whether the inheritance attribute of a collection is true.

    Parameters:
      cm - The iRODS context map
      path - The path being checked."
  (validate-path-lengths path)
  (when (is-dir? cm path)
    (.isCollectionSetForPermissionInheritance (:collectionAO cm) path)))

(defn is-writeable?
  [cm user path]
  "Returns true if 'user' can write to 'path'.

    Parameters:
      cm - The iRODS context map
      user - String containign a username.
      path - String containing an absolute path for something in iRODS."
  (validate-path-lengths path)
  (cond
   (not (user-exists? cm user))
   false

   (is-dir? cm path)
   (collection-writeable? cm user (ft/rm-last-slash path))

   (is-file? cm path)
   (dataobject-writeable? cm user (ft/rm-last-slash path))

   :else
   false))

(defn is-readable?
  [cm user path]
  "Returns true if 'user' can read 'path'.

    Parameters:
      cm - The iRODS context map
      user - String containing a username.
      path - String containing an path for something in iRODS."
  (validate-path-lengths path)
  (cond
   (not (user-exists? cm user))
   false

   (is-dir? cm path)
   (collection-readable? cm user (ft/rm-last-slash path))

   (is-file? cm path)
   (dataobject-readable? cm user (ft/rm-last-slash path))

   :else
   false))

(defn last-dir-in-path
  [cm path]
  "Returns the name of the last directory in 'path'.

    Please note that this function works by calling
    getCollectionLastPathComponent on a Collection instance and therefore
    hits iRODS every time you call it. Don't call this from within a loop.

    Parameters:
      cm - The iRODS context map
      path - String containing the path for an item in iRODS.

    Returns:
      String containing the name of the last directory in the path."
  (validate-path-lengths path)
  (.getCollectionLastPathComponent
    (.findByAbsolutePath (:collectionAO cm) (ft/rm-last-slash path))))

(defn sub-collections
  [cm path]
  "Returns a sequence of Collections that reside directly in the directory
    refered to by 'path'.

    Parameters:
      cm - The iRODS context map
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing Collections (the Jargon kind) representing
      directories that reside under the directory represented by 'path'."
  (validate-path-lengths path)
  (.listCollectionsUnderPath (:lister cm) (ft/rm-last-slash path) 0))

(defn sub-collection-paths
  [cm path]
  "Returns a sequence of string containing the paths for directories
    that live under 'path' in iRODS.

    Parameters:
      cm - The iRODS context map
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing the paths for directories that live under 'path'."
  (validate-path-lengths path)
  (map
    #(.getFormattedAbsolutePath %)
    (sub-collections cm path)))

(defn sub-dir-maps
  [cm user list-obj filter-files]
  (let [abs-path (.getFormattedAbsolutePath list-obj)
        basename (ft/basename abs-path)
        lister   (:lister cm)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (collection-perm-map cm user abs-path)
     :hasSubDirs    (pos? (count (.listCollectionsUnderPath lister abs-path 0)))
     :date-created  (str (long (.. list-obj getCreatedAt getTime)))
     :date-modified (str (long (.. list-obj getModifiedAt getTime)))}))

(defn sub-file-maps
  [cm user list-obj]
  (let [abs-path (.getFormattedAbsolutePath list-obj)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (dataobject-perm-map cm user abs-path)
     :date-created  (str (long (.. list-obj getCreatedAt getTime)))
     :date-modified (str (long (.. list-obj getModifiedAt getTime)))
     :file-size     (str (.getDataSize list-obj))}))

(defn paths-writeable?
  [cm user paths]
  "Returns true if all of the paths in 'paths' are writeable by 'user'.

    Parameters:
      cm - The iRODS context map
      user - A string containing the username of the user requesting the check.
      paths - A sequence of strings containing the paths to be checked."
  (doseq [p paths] (validate-path-lengths p))
  (reduce
    #(and %1 %2)
    (map
      #(is-writeable? cm user %)
      paths)))

;;Metadata

(defn map2avu
  [avu-map]
  "Converts an avu map into an AvuData instance."
  (AvuData/instance (:attr avu-map) (:value avu-map) (:unit avu-map)))

(defn get-metadata
  [cm dir-path]
  "Returns all of the metadata associated with a path."
  (validate-path-lengths dir-path)
  (mapv
    #(hash-map :attr  (.getAvuAttribute %1)
               :value (.getAvuValue %1)
               :unit  (.getAvuUnit %1))
    (if (is-dir? cm dir-path)
      (.findMetadataValuesForCollection (:collectionAO cm) dir-path)
      (.findMetadataValuesForDataObject (:dataObjectAO cm) dir-path))))

(defn get-attribute
  [cm dir-path attr]
  "Returns a list of avu maps for set of attributes associated with dir-path"
  (validate-path-lengths dir-path)
  (filter
    #(= (:attr %1) attr)
    (get-metadata cm dir-path)))

(defn attribute?
  [cm dir-path attr]
  "Returns true if the path has the associated attribute."
  (validate-path-lengths dir-path)
  (pos? (count (get-attribute cm dir-path attr))))

(defn add-metadata
  [cm dir-path attr value unit]
  (validate-path-lengths dir-path)
  (let [ao-obj (if (is-dir? cm dir-path)
                 (:collectionAO cm)
                 (:dataObjectAO cm))]
    (.addAVUMetadata ao-obj dir-path (AvuData/instance attr value unit))))

(defn set-metadata
  [cm dir-path attr value unit]
  "Sets an avu for dir-path."
  (validate-path-lengths dir-path)
  (let [avu    (AvuData/instance attr value unit)
        ao-obj (if (is-dir? cm dir-path)
                 (:collectionAO cm)
                 (:dataObjectAO cm))]
    (if (zero? (count (get-attribute cm dir-path attr)))
      (.addAVUMetadata ao-obj dir-path avu)
      (let [old-avu (map2avu (first (get-attribute cm dir-path attr)))]
        (.modifyAVUMetadata ao-obj dir-path old-avu avu)))))

(defn delete-metadata
  [cm dir-path attr]
  "Deletes an avu from dir-path."
  (validate-path-lengths dir-path)
  (let [fattr  (first (get-attribute cm dir-path attr))
        avu    (map2avu fattr)
        ao-obj (if (is-dir? cm dir-path)
                 (:collectionAO cm)
                 (:dataObjectAO cm))]
    (.deleteAVUMetadata ao-obj dir-path avu)))

(defn- op->constant
  [op]
  (or ({:between         QueryConditionOperators/BETWEEN
        :=               QueryConditionOperators/EQUAL
        :>               QueryConditionOperators/GREATER_THAN
        :>=              QueryConditionOperators/GREATER_THAN_OR_EQUAL_TO
        :in              QueryConditionOperators/IN
        :<               QueryConditionOperators/LESS_THAN
        :<=              QueryConditionOperators/LESS_THAN_OR_EQUAL_TO
        :like            QueryConditionOperators/LIKE
        :not-between     QueryConditionOperators/NOT_BETWEEN
        :not=            QueryConditionOperators/NOT_EQUAL
        :not-in          QueryConditionOperators/NOT_IN
        :not-like        QueryConditionOperators/NOT_LIKE
        :num=            QueryConditionOperators/NUMERIC_EQUAL
        :num>            QueryConditionOperators/NUMERIC_GREATER_THAN
        :num>=           QueryConditionOperators/NUMERIC_GREATER_THAN_OR_EQUAL_TO
        :num<            QueryConditionOperators/NUMERIC_LESS_THAN
        :num<=           QueryConditionOperators/NUMERIC_LESS_THAN_OR_EQUAL_TO
        :sounds-like     QueryConditionOperators/SOUNDS_LIKE
        :sounds-not-like QueryConditionOperators/SOUNDS_NOT_LIKE
        :table           QueryConditionOperators/TABLE} op)
      (throw (Exception. (str "unknown operator: " op)))))

(defn- build-file-avu-query
  [name op value]
  (-> (IRODSGenQueryBuilder. true nil)
      (.addSelectAsGenQueryValue RodsGenQueryEnum/COL_COLL_NAME)
      (.addSelectAsGenQueryValue RodsGenQueryEnum/COL_DATA_NAME)
      (.addConditionAsGenQueryField RodsGenQueryEnum/COL_META_DATA_ATTR_NAME
                                    QueryConditionOperators/EQUAL name)
      (.addConditionAsGenQueryField RodsGenQueryEnum/COL_META_DATA_ATTR_VALUE
                                    (op->constant op) value)
      (.exportIRODSQueryFromBuilder 50000)))

(defn build-query-for-avu-by-obj
  [file-path attr op value]
  (-> (IRODSGenQueryBuilder. true nil)
      (.addSelectAsGenQueryValue RodsGenQueryEnum/COL_META_DATA_ATTR_NAME)
      (.addSelectAsGenQueryValue RodsGenQueryEnum/COL_META_DATA_ATTR_VALUE)
      (.addSelectAsGenQueryValue RodsGenQueryEnum/COL_META_DATA_ATTR_UNITS)
      #_(.addSelectAsGenQueryValue RodsGenQueryEnum/COL_COLL_NAME)
      #_(.addSelectAsGenQueryValue RodsGenQueryEnum/COL_DATA_NAME)
      (.addConditionAsGenQueryField RodsGenQueryEnum/COL_META_DATA_ATTR_NAME
                                    QueryConditionOperators/EQUAL attr)
      (.addConditionAsGenQueryField RodsGenQueryEnum/COL_META_DATA_ATTR_VALUE
                                    (op->constant op) value)
      (.exportIRODSQueryFromBuilder 50000)))

(defn list-files-with-avu
  [cm name op value]
  (let [query    (build-file-avu-query name op value)
        rs       (.executeIRODSQueryAndCloseResult (:executor cm) query 0)]
    (map #(string/join "/" (.getColumnsAsList %)) (.getResults rs))))

(def ^:private file-avu-query-columns
  {:name  RodsGenQueryEnum/COL_META_DATA_ATTR_NAME
   :value RodsGenQueryEnum/COL_META_DATA_ATTR_VALUE
   :unit  RodsGenQueryEnum/COL_META_DATA_ATTR_UNITS})

(def ^:private dir-avu-query-columns
  {:name  RodsGenQueryEnum/COL_META_COLL_ATTR_NAME
   :value RodsGenQueryEnum/COL_META_COLL_ATTR_VALUE
   :unit  RodsGenQueryEnum/COL_META_COLL_ATTR_UNITS})

(defn- add-conditions-from-avu-spec
  "Adds conditions from an AVU specification to a general query builder. The query specification
   is a map in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values."
  [cols builder avu-spec]
  (->> (remove (comp nil? last) avu-spec)
       (map (fn [[k v]] [(cols k) v]))
       (remove (comp nil? first))
       (map
        (fn [[col v]]
          (.addConditionAsGenQueryField builder col QueryConditionOperators/EQUAL v)))
       (dorun)))

(defn- build-subtree-query-from-avu-spec
  "Builds a subtree query from a path and an AVU specification.  The AVU specification is a map
   in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values.

   The path is the absolute path to the root of the subtree to search. Items that are not in this
   directory or any of its descendants will not be matched. The root of the subtree is included
   in the search."
  [select-columns condition-columns path avu-spec]
  (let [builder (IRODSGenQueryBuilder. true nil)]
    (dorun (map #(.addSelectAsGenQueryValue builder %) select-columns))
    (when path
      (.addConditionAsGenQueryField builder
                                    RodsGenQueryEnum/COL_COLL_NAME
                                    QueryConditionOperators/LIKE
                                    (str path \%)))
    (add-conditions-from-avu-spec condition-columns builder avu-spec)
    (.exportIRODSQueryFromBuilder builder 50000)))

(defn- list-items-in-tree-with-attr
  "Lists either files or directories in a subtree given the path to the root of the subtree and an
   AVU specification. The AVU specification is a map in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values.

   The path is the absolute path to the root of the subtree to search. Items that are not in this
   directory or any of its descendants will not be matched. The root of the subtree is included
   in the search.

   The select-columns parameter indicates which columns should be selected from the query.  The
   condition-columns parameter is a map indicating which constants to use in the query for the
   :name, :value, and :unit elements of the AVU specification.  The format-row parameter is a
   function that can be used to format each row in the result set.  The single parameter to this
   function is an instance of IRODSQueryResultRow."
  [select-columns condition-columns format-row cm path avu-spec]
  (let [query (build-subtree-query-from-avu-spec select-columns condition-columns path avu-spec)]
    (->> (.executeIRODSQueryAndCloseResult (:executor cm) query 0)
         (.getResults)
         (mapv format-row))))

(def list-files-in-tree-with-attr
  "Lists the paths to files in a subtree given the path to the root of the subtree and an AVU
   specification. The AVU specification is a map in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values.

   The path is the absolute path to the root of the subtree to search. Items that are not in this
   directory or any of its descendants will not be matched. The root of the subtree is included
   in the search."
  (partial list-items-in-tree-with-attr
           [RodsGenQueryEnum/COL_COLL_NAME RodsGenQueryEnum/COL_DATA_NAME]
           file-avu-query-columns
           #(string/join "/" (.getColumnsAsList %))))

(def list-collections-in-tree-with-attr
  "Lists the paths to directories in a subtree given the path to the root of the subtree and an
   AVU specification. The AVU specification is a map in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values.

   The path is the absolute path to the root of the subtree to search. Items that are not in this
   directory or any of its descendants will not be matched. The root of the subtree is included
   in the search."
  (partial list-items-in-tree-with-attr
           [RodsGenQueryEnum/COL_COLL_NAME]
           dir-avu-query-columns
           #(str (first (.getColumnsAsList %)))))

(defn list-everything-in-tree-with-attr
  [cm path avu-spec]
  "Lists the paths to both files and directories in a subtree given the path to the root of the
   subtree and an AVU specification. The AVU specification is a map in the following format:

       {:name  \"name\"
        :value \"value\"
        :unit  \"unit\"}

   The values in the map are strings indicating the name, value or unit of the AVUs to match. Each
   entry in the map is optional, so that the caller can search for any combination of name and
   value. For example, to search for AVUs named 'foo', the AVU specification would simply be
   {:name \"foo\"}. Unrecognized keys in the AVU specification are currently ignored and conditions
   are not added for null values.

   The path is the absolute path to the root of the subtree to search. Items that are not in this
   directory or any of its descendants will not be matched. The root of the subtree is included
   in the search."
  (doall (mapcat #(% cm path avu-spec)
                 [list-collections-in-tree-with-attr list-files-in-tree-with-attr])))

(defn get-avus-by-collection
  "Returns AVUs associated with a collection that have the given attribute and value."
  [cm file-path attr units]
  (let [query [(AVUQueryElement/instanceForValueQuery
                AVUQueryElement$AVUQueryPart/UNITS
                AVUQueryOperatorEnum/EQUAL
                units)
               (AVUQueryElement/instanceForValueQuery
                AVUQueryElement$AVUQueryPart/ATTRIBUTE
                AVUQueryOperatorEnum/EQUAL
                attr)]]
    (mapv
     #(hash-map
       :attr  (.getAvuAttribute %1)
       :value (.getAvuValue %1)
       :unit  (.getAvuUnit %1))
     (.findMetadataValuesByMetadataQueryForCollection (:collectionAO cm) query file-path))))

(defn list-collections-with-attr-units
  [cm attr units]
  (let [query [(AVUQueryElement/instanceForValueQuery
                AVUQueryElement$AVUQueryPart/UNITS
                AVUQueryOperatorEnum/EQUAL
                units)
               (AVUQueryElement/instanceForValueQuery
                AVUQueryElement$AVUQueryPart/ATTRIBUTE
                AVUQueryOperatorEnum/EQUAL
                attr)]]
    (mapv
     #(.getCollectionName %)
     (.findDomainByMetadataQuery (:collectionAO cm) query))))

(defn list-all
  [cm dir-path]
  (validate-path-lengths dir-path)
  (.listDataObjectsAndCollectionsUnderPath (:lister cm) dir-path))

(defn mkdir
  [cm dir-path]
  (validate-full-dirpath dir-path)
  (validate-path-lengths dir-path)
  (.mkdir (:fileSystemAO cm) (file cm dir-path) true))

(defn mkdirs
  [cm dir-path]
  (validate-full-dirpath dir-path)
  (validate-path-lengths dir-path)
  (.mkdirs (file cm dir-path)))

(defn delete
  [cm a-path]
  (validate-path-lengths a-path)
  (let [fileSystemAO (:fileSystemAO cm)
        resource     (file cm a-path)]
    (if (:use-trash cm)
      (if (is-dir? cm a-path)
        (.directoryDeleteNoForce fileSystemAO resource)
        (.fileDeleteNoForce fileSystemAO resource))
      (if (is-dir? cm a-path)
        (.directoryDeleteForce fileSystemAO resource)
        (.fileDeleteForce fileSystemAO resource)))))

(defn log-last
  [item]
  (log/warn "process-perms: " item)
  item)

(defn process-perms
  [f cm path user admin-users]
  (->> (list-user-perms cm path)
    (log-last)
    (remove #(contains? (set (conj admin-users user (:username cm))) (:user %)))
    (log-last)
    (map f)
    (log-last)
    (dorun)))

(defn process-parent-dirs
  [f process? path]
  (log/warn "in process-parent-dirs")
  (loop [dir-path (ft/dirname path)]
    (log/warn "processing path " dir-path)
    (when (process? dir-path)
      (log/warn "processing directory:" dir-path)
      (f dir-path)
      (recur (ft/dirname dir-path)))))

(defn set-readable
  [cm username readable? path]
  (let [{curr-write :write curr-own :own} (permissions cm username path)]
    (set-permissions cm username path readable? curr-write curr-own)))

(defn contains-accessible-obj?
  [cm user dpath]
  (log/warn "in contains-accessible-obj? - " user " " dpath)
  (log/warn "results of list-paths " (list-paths cm dpath))
  (some #(is-readable? cm user %1) (list-paths cm dpath)))

(defn reset-perms
  [cm path user admin-users]
  (process-perms
   #(set-permissions cm (:user %) path false false false true)
   cm path user admin-users))

(defn inherit-perms
  [cm path user admin-users]
  (let [parent (ft/dirname path)]
    (process-perms
     (fn [{sharee :user {r :read w :write o :own} :permissions}]
       (set-permissions cm sharee path r w o true))
     cm parent user admin-users)))

(defn remove-obsolete-perms
  "Removes permissions that are no longer required for a directory that isn't shared.  Read
   permissions are no longer required for any user who no longer has access to any file or
   subdirectory."
  [cm path user admin-users]
  (let [parent    (ft/dirname path)
        base-dirs #{(ft/rm-last-slash (:home cm)) (trash-base-dir cm)}]
    (process-perms
     (fn [{sharee :user}]
       (process-parent-dirs
        (partial set-readable cm sharee false)
        #(and (not (base-dirs %)) (not (contains-accessible-obj? cm sharee %)))
        path))
     cm parent user admin-users)))

(defn make-file-accessible
  "Ensures that a file is accessible to all users that have access to the file."
  [cm path user admin-users]
  (let [parent    (ft/dirname path)
        base-dirs #{(ft/rm-last-slash (:home cm)) (trash-base-dir cm)}]
    (process-perms
     (fn [{sharee :user}]
       (process-parent-dirs (partial set-readable cm sharee true) #(not (base-dirs %)) path))
     cm path user admin-users)))

(def ^:private perm-fix-fns
  "Functions used to update permissions after something is moved, indexed by the inherit flag
   of the source directory followed by the destination directory."
  {true  {true  (fn [cm src dst user admin-users skip-source-perms?]
                  (reset-perms cm dst user admin-users)
                  (inherit-perms cm dst user admin-users))

          false (fn [cm src dst user admin-users skip-source-perms?]
                  (reset-perms cm dst user admin-users))}

   false {true  (fn [cm src dst user admin-users skip-source-perms?]
                  (when-not skip-source-perms?
                    (remove-obsolete-perms cm src user admin-users))
                  (reset-perms cm dst user admin-users)
                  (inherit-perms cm dst user admin-users))

          false (fn [cm src dst user admin-users skip-source-perms?]
                  (when-not skip-source-perms?
                    (remove-obsolete-perms cm src user admin-users))
                  (make-file-accessible cm dst user admin-users))}})

(defn fix-perms
  [cm src dst user admin-users skip-source-perms?]
  (let [src-dir       (ft/dirname src)
        dst-dir       (ft/dirname dst)]
    (when-not (= src-dir dst-dir)
      ((get-in perm-fix-fns (mapv #(permissions-inherited? cm %) [src-dir dst-dir]))
       cm (.getPath src) (.getPath dst) user admin-users skip-source-perms?))))

(defn move
  "Moves a file/dir from source path 'source' into destination directory 'dest'.

   Parameters:
     source - String containing the path to the file/dir being moved.
     dest - String containing the path to the destination directory. Should not end with a slash.
     :admin-users (optional) - List of users that must retain ownership on the file/dir being moved.
     :user (optional) - The username of the user performing the move.
     :skip-source-perms? (optional) - Boolean the tells move to skip ensuring that permissions for
                                      the admin users are correct."
  [cm source dest & {:keys [admin-users user skip-source-perms?]
                     :or   {admin-users #{}
                            skip-source-perms? false}}]
  (validate-path-lengths source)
  (validate-path-lengths dest)

  (let [fileSystemAO (:fileSystemAO cm)
        src          (file cm source)
        dst          (file cm dest)]

    (if (is-file? cm source)
      (.renameFile fileSystemAO src dst)
      (.renameDirectory fileSystemAO src dst))

    (fix-perms cm src dst user admin-users skip-source-perms?)))

(defn move-all
  [cm sources dest & {:keys [admin-users user]
                      :or {admin-users #{}}}]
  (doseq [s sources] (validate-path-lengths (ft/path-join dest (ft/basename s))))

  (dorun
   (map
    #(move cm %1 (ft/path-join dest (ft/basename %1)) :user user :admin-users admin-users)
    sources)))

(defn output-stream
  "Returns an FileOutputStream for a file in iRODS pointed to by 'output-path'."
  [cm output-path]
  (validate-path-lengths output-path)
  (.instanceIRODSFileOutputStream (:fileFactory cm) (file cm output-path)))

(defn input-stream
  "Returns a FileInputStream for a file in iRODS pointed to by 'input-path'"
  [cm input-path]
  (validate-path-lengths input-path)
  (.instanceIRODSFileInputStream (:fileFactory cm) (file cm input-path)))

(defn proxy-input-stream
  [cm istream]
  (proxy [java.io.InputStream] []
    (available [] (.available istream))
    (mark [readlimit] (.mark istream readlimit))
    (markSupported [] (.markSupported istream))
    (read
      ([] (.read istream))
      ([b] (.read istream b))
      ([b off len] (.read istream b off len)))
    (reset [] (.reset istream))
    (skip [] (.skip istream))
    (close []
      (.close istream)
      (.close (:fileSystem cm)))))

(defn read-file
  [cm fpath buffer]
  (validate-path-lengths fpath)
  (.read (IRODSFileReader. (file cm fpath) (:fileFactory cm)) buffer))

(defn shopping-cart
  [filepaths]
  (doseq [fp filepaths] (validate-path-lengths fp))

  (let [cart (FileShoppingCart/instance)]
    (loop [fps filepaths]
      (.addAnItem cart (ShoppingCartEntry/instance (first fps)))
      (if (pos? (count (rest fps)))
        (recur (rest fps))))
    cart))

(defn temp-password
  [cm user]
  (.getTemporaryPasswordForASpecifiedUser (:userAO cm) user))

(defn cart-service
  [cm]
  (ShoppingCartServiceImpl.
    (:accessObjectFactory cm)
    (:irodsAccount cm)
    (DataCacheServiceFactoryImpl. (:accessObjectFactory cm))))

(defn store-cart
  [cm user cart-key filepaths]
  (doseq [fp filepaths] (validate-path-lengths fp))
  (.serializeShoppingCartAsSpecifiedUser
    (cart-service cm)
    (shopping-cart filepaths)
    cart-key
    user))

(defn permissions
  [cm user fpath]
  (validate-path-lengths fpath)
  (cond
    (is-dir? cm fpath)
    (collection-perm-map cm user fpath)

    (is-file? cm fpath)
    (dataobject-perm-map cm user fpath)

    :else
    {:read false
     :write false
     :own false}))

(defn remove-permissions
  [cm user fpath]
  (validate-path-lengths fpath)
  (cond
   (is-file? cm fpath)
   (.removeAccessPermissionsForUserInAdminMode
     (:dataObjectAO cm)
     (:zone cm)
     fpath
     user)

   (is-dir? cm fpath)
   (.removeAccessPermissionForUserAsAdmin
     (:collectionAO cm)
     (:zone cm)
     fpath
     user
     true)))

(defn owns?
  [cm user fpath]
  (validate-path-lengths fpath)
  (cond
    (is-file? cm fpath)
    (owns-dataobject? cm user fpath)

    (is-dir? cm fpath)
    (owns-collection? cm user fpath)

    :else
    false))

(defn remove-access-permissions
  [cm user abs-path]
  (validate-path-lengths abs-path)
  (cond
   (is-file? cm abs-path)
   (.removeAccessPermissionsForUserInAdminMode
     (:dataObjectAO cm)
     (:zone cm)
     abs-path
     user)

   (is-dir? cm abs-path)
   (.removeAccessPermissionForUserAsAdmin
     (:collectionAO cm)
     (:zone cm)
     abs-path
     user
     false)))

(defn removed-owners
  [curr-user-perms set-of-new-owners]
  (filterv
    #(not (contains? set-of-new-owners %1))
    (map :user curr-user-perms)))

(defn fix-owners
  [cm abs-path & owners]
  (validate-path-lengths abs-path)
  (let [curr-user-perms   (list-user-perms cm abs-path)
        set-of-new-owners (set owners)
        rm-zone           #(if (string/split %1 #"\#")
                             (first (string/split %1 #"\#"))
                             "")]
    (doseq [non-user (filterv
                      #(not (contains? set-of-new-owners %1))
                      (map :user curr-user-perms))]
      (remove-access-permissions cm non-user abs-path))

    (doseq [new-owner set-of-new-owners]
      (set-owner cm abs-path new-owner))))

(defn ticket-admin-service
  "Creates an instance of TicketAdminService, which provides
   access to utility methods for performing operations on tickets.
   Probably doesn't need to be called directly."
  [cm user]
  (let [tsf (TicketServiceFactoryImpl. (:accessObjectFactory cm))]
    (.instanceTicketAdminService tsf (account cm user (temp-password cm user)))))

(defn set-ticket-options
  "Sets the optional settings for a ticket, such as the expiration date
   and the uses limit."
  [ticket-id tas
   {:keys [byte-write-limit expiry file-write-limit uses-limit]}]
  (when byte-write-limit
    (.setTicketByteWriteLimit tas ticket-id byte-write-limit))
  (when expiry
    (.setTicketExpiration tas ticket-id expiry))
  (when file-write-limit
    (.setTicketFileWriteLimit tas ticket-id file-write-limit))
  (when uses-limit
    (.setTicketUsesLimit tas ticket-id uses-limit)))

(defn create-ticket
  [cm user fpath ticket-id & {:as ticket-opts}]
  (validate-path-lengths fpath)
  (let [tas        (ticket-admin-service cm user)
        read-mode  TicketCreateModeEnum/READ
        new-ticket (.createTicket tas read-mode (file cm fpath) ticket-id)]
    (set-ticket-options ticket-id tas ticket-opts)
    new-ticket))

(defn modify-ticket
  [cm user ticket-id & {:as ticket-opts}]
  (set-ticket-options ticket-id (ticket-admin-service cm user) ticket-opts))

(defn delete-ticket
  "Deletes the ticket specified by ticket-id."
  [cm user ticket-id]
  (.deleteTicket (ticket-admin-service cm user) ticket-id))

(defn ticket?
  "Checks to see if ticket-id is already being used as a ticket
   identifier."
  [cm user ticket-id]
  (.isTicketInUse (ticket-admin-service cm user) ticket-id))

(defn ticket-by-id
  "Looks up the ticket by the provided ticket-id string and
   returns an instance of Ticket."
  [cm user ticket-id]
  (.getTicketForSpecifiedTicketString
    (ticket-admin-service cm user)
    ticket-id))

(defn ticket-obj->map
  [ticket]
  {:ticket-id        (.getTicketString ticket)
   :byte-write-limit (str (.getWriteByteLimit ticket))
   :byte-write-count (str (.getWriteByteCount ticket))
   :uses-limit       (str (.getUsesLimit ticket))
   :uses-count       (str (.getUsesCount ticket))
   :file-write-limit (str (.getWriteFileLimit ticket))
   :file-write-count (str (.getWriteFileCount ticket))
   :expiration       (or (.getExpireTime ticket) "")})

(defn ticket-map
  [cm user ticket-id]
  (ticket-obj->map (ticket-by-id cm user ticket-id)))

(defn ticket-ids-for-path
  [cm user path]
  (let [tas (ticket-admin-service cm user)]
    (if (is-dir? cm path)
      (mapv ticket-obj->map (.listAllTicketsForGivenCollection tas path 0))
      (mapv ticket-obj->map (.listAllTicketsForGivenDataObject tas path 0)))))

(defn ticket-expired?
  [ticket-obj]
  (if (.getExpireTime ticket-obj)
    (.. (java.util.Date.) (after (.getExpireTime ticket-obj)))
    false))

(defn ticket-used-up?
  [ticket-obj]
  (> (.getUsesCount ticket-obj) (.getUsesLimit ticket-obj)))

(defn init-ticket-session
  [cm ticket-id]
  (.. (:accessObjectFactory cm)
    getIrodsSession
    (currentConnection (:irodsAccount cm))
    (irodsFunction
      (TicketInp/instanceForSetSessionWithTicket ticket-id))))

(defn ticket-input-stream
  [cm user ticket-id]
  (init-ticket-session cm ticket-id)
  (input-stream cm (.getIrodsAbsolutePath (ticket-by-id cm user ticket-id))))

(defn quota-map
  [quota-entry]
  (hash-map
    :resource (.getResourceName quota-entry)
    :zone     (.getZoneName quota-entry)
    :user     (.getUserName quota-entry)
    :updated  (str (.getTime (.getUpdatedAt quota-entry)))
    :limit    (str (.getQuotaLimit quota-entry))
    :over     (str (.getQuotaOver quota-entry))))

(defn quota
  [cm user]
  (mapv quota-map (.listQuotaForAUser (:quotaAO cm) user)))

(defn data-transfer-obj
  [cm]
  (.getDataTransferOperations (:accessObjectFactory cm) (:irodsAccount cm)))

(defn copy
  [cm source dest]
  (validate-path-lengths source)
  (validate-path-lengths dest)

  (let [dto (data-transfer-obj cm)
        res (or (:defaultResource cm) "demoResc")]
    (.copy dto source res dest nil nil)))

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
        buffer      (byte-array num-bytes)]
    (doto access-file
      (.seek position SEEK-CURRENT)
      (.read buffer)
      (.close))
    (bytes->string buffer)))

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

(defmacro with-jargon
  "An iRODS connection is opened, binding the connection's context to the symbolic cm-sym value.
   Next it evaluates the body expressions. Finally, it closes the iRODS connection*. The body
   expressions should use the value of cm-sym to access the iRODS context.

   Parameters:
     cfg - The Jargon configuration used to connect to iRODS.
     [cm-sym] - Holds the name of the binding to the iRODS context map used by the body expressions.
     body - Zero or more expressions to be evaluated while an iRODS connection is open.

   Returns:
     It returns the result from evaluating the last expression in the body.*

    Throws:
      org.irods.jargon.core.exception.JargonException - This is thrown when if fails to connect to iRODS

    Example:
      (def config (init ...))

     (with-jargon config
       [ctx]
       (list-all ctx \"/zone/home/user/\"))

   * If an IRODSFileInputStream is the result of the last body expression, the iRODS connection is
     not closed. Instead, a special InputStream is returned than when closed, closes the iRODS
     connection is well."
  [cfg [cm-sym] & body]
  `(binding [curr-with-jargon-index (dosync (alter with-jargon-index inc))]
     (log/debug "curr-with-jargon-index:" curr-with-jargon-index)
     (when-let [~cm-sym (create-jargon-context-map ~cfg)]
       (ss/try+
         (let [retval# (do ~@body)]
           (if (instance? IRODSFileInputStream retval#)
             (do (log/debug curr-with-jargon-index "- returning a proxy input stream...")
                 (proxy-input-stream ~cm-sym retval#)) ;The proxied InputStream handles clean up.
             (do (log/debug curr-with-jargon-index "- cleaning up and returning a plain value")
                 (clean-return ~cm-sym retval#))))
         (catch Object o1#
           (ss/try+
             (.close (:fileSystem ~cm-sym))
             (catch Object o2#))
           (ss/throw+))))))

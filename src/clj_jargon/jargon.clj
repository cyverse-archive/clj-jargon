(ns clj-jargon.jargon
  (:require [clojure-commons.file-utils :as ft]
            [clojure.tools.logging :as log]
            [clojure.string :as string])
  (:import [org.irods.jargon.core.exception DataNotFoundException]
           [org.irods.jargon.core.protovalues FilePermissionEnum]
           [org.irods.jargon.core.pub.domain AvuData]
           [org.irods.jargon.core.connection IRODSAccount]
           [org.irods.jargon.core.pub IRODSFileSystem]
           [org.irods.jargon.core.pub.io 
            IRODSFileReader 
            IRODSFileInputStream]
           [org.irods.jargon.datautils.datacache 
            DataCacheServiceFactoryImpl]
           [org.irods.jargon.datautils.shoppingcart 
            FileShoppingCart
            ShoppingCartEntry 
            ShoppingCartServiceImpl]
           [java.io FileInputStream]
           [org.irods.jargon.ticket 
            TicketServiceFactoryImpl 
            TicketAdminServiceImpl
            TicketClientSupport]
           [org.irods.jargon.ticket.packinstr
            TicketInp
            TicketCreateModeEnum]))

(def read-perm FilePermissionEnum/READ)
(def write-perm FilePermissionEnum/WRITE)
(def own-perm FilePermissionEnum/OWN)
(def none-perm FilePermissionEnum/NONE)

; Configuration settings for iRODS/Jargon
(def host (atom ""))
(def port (atom 0))
(def username (atom ""))
(def password (atom ""))
(def home (atom ""))
(def zone (atom ""))
(def defaultResource (atom ""))
(def irodsaccount (atom nil))
(def conn-map (atom nil))
(def fileSystem (atom nil))
(def max-retries (atom 0))
(def retry-sleep (atom 0))
(def use-trash (atom false))

; Debuging code.
(def with-jargon-index (ref 0))
(def ^:dynamic curr-with-jargon-index nil)

;set up the thread-local var
(def ^:dynamic cm nil)

(defn init
  "Resets the connection config atoms with the values passed in."
  ([ahost aport auser apass ahome azone ares]
    (init ahost aport auser apass ahome azone ares 0 0 false))
  ([ahost aport auser apass ahome azone ares num-retries sleep recycle]
    (reset! host ahost)
    (reset! port aport)
    (reset! username auser)
    (reset! password apass)
    (reset! home ahome)
    (reset! zone azone)
    (reset! defaultResource ares)
    (reset! max-retries num-retries)
    (reset! retry-sleep sleep)
    (reset! use-trash recycle)
    nil))

(defn account
  ([]
    (account @username @password))
  ([user pass]
    (IRODSAccount. @host (Integer/parseInt @port) user pass @home @zone @defaultResource)))

(defn clean-return
  [retval]
  (.close (:fileSystem cm))
  retval)

(defn- context-map
  []
  (let [account     (account)
        file-system (IRODSFileSystem/instance)
        aof         (.getIRODSAccessObjectFactory file-system)]
    {:irodsAccount        account
     :fileSystem          file-system
     :accessObjectFactory aof
     :collectionAO        (.getCollectionAO aof account)
     :dataObjectAO        (.getDataObjectAO aof account)
     :userAO              (.getUserAO aof account)
     :userGroupAO         (.getUserGroupAO aof account)
     :fileFactory         (.getIRODSFileFactory file-system account)
     :fileSystemAO        (.getIRODSFileSystemAO aof account)
     :lister              (.getCollectionAndDataObjectListAndSearchAO aof account)
     :quotaAO             (.getQuotaAO aof account)
     :home                @home
     :zone                @zone}))

(defn- log-value
  [msg value]
  (log/debug curr-with-jargon-index "-" msg value)
  value)

(defn- get-context
  []
  (let [retval {:succeeded true :retval nil :exception nil :retry false}]
    (try
      (log-value "retval:" (assoc retval :retval (context-map)))
      (catch java.net.ConnectException e
        (log/debug curr-with-jargon-index "- caught a ConnectException:" e)
        (log/debug curr-with-jargon-index "- need to retry...")
        (assoc retval :exception e :succeeded false :retry true))
      (catch java.lang.Exception e
        (log/debug curr-with-jargon-index "- got an Exception:" e)
        (log/debug curr-with-jargon-index "- shouldn't retry...")
        (assoc retval :exception e :succeeded false :retry false)))))

(defn create-jargon-context-map
  "Creates a map containing instances of commonly used Jargon objects."
  []
  (loop [num-tries 0]
    (let [retval (get-context)
          error? (not (:succeeded retval))
          retry? (:retry retval)]
      (cond
        (and error? retry? (< num-tries @max-retries))
        (do (Thread/sleep @retry-sleep)
          (recur (inc num-tries)))
        
        error?
        (throw (:exception retval))
        
        :else (:retval retval)))))

(defn user-groups
  "Returns a list of group names that the user is in."
  [user]
  (for [ug (.findUserGroupsForUser (:userGroupAO cm) user)]
    (.getUserGroupName ug)))

(defn user-dataobject-perms
  "Returns a set of permissions that user has for the dataobject at
   data-path. Takes into account the groups that a user is in."
  [user data-path]
  (let [user-groups  (user-groups user)
        zone         (:zone cm)
        dataObjectAO (:dataObjectAO cm)]
    (set 
      (filterv
        #(not= %1 none-perm)
        (for [username user-groups]
          (.getPermissionForDataObject dataObjectAO data-path username zone))))))

(defn user-collection-perms
  "Returns a set of permissions that a user has for the collection at
   data-path. Takes into account the groups that a user is in. "
  [user coll-path]
  (let [user-groups  (user-groups user)
        zone         (:zone cm)
        collectionAO (:collectionAO cm)]
    (set 
      (filterv
        #(not= %1 none-perm)
        (for [username user-groups]
          (.getPermissionForCollection collectionAO coll-path username zone))))))

(defn dataobject-perm-map
  "Uses (user-dataobject-perms) to grab the 'raw' permissions for
   the user for the dataobject at data-path, and returns a map with
   the keys :read :write and :own. The values are booleans."
  [user data-path]
  (let [perms  (user-dataobject-perms user data-path)
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
  [user coll-path]
  (let [perms  (user-collection-perms user coll-path)
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
  [username data-path checked-perm]
  (let [perms (user-dataobject-perms username data-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn dataobject-readable?
  "Checks to see if the user has read permissions on data-path. Only
   works for dataobjects."
  [user data-path]
  (dataobject-perm? user data-path read-perm))
  
(defn dataobject-writeable?
  "Checks to see if the user has write permissions on data-path. Only
   works for dataobjects."
  [user data-path]
  (dataobject-perm? user data-path write-perm))

(defn owns-dataobject?
  "Checks to see if the user has ownership permissions on data-path. Only
   works for dataobjects."
  [user data-path]
  (dataobject-perm? user data-path own-perm))

(defn collection-perm?
  "Utility function that checks to see if the user has the specified
   permission for the collection path."
  [username coll-path checked-perm]
  (let [perms (user-collection-perms username coll-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn collection-readable?
  "Checks to see if the user has read permissions on coll-path. Only
   works for collection paths."
  [user coll-path]
  (or (collection-perm? user coll-path read-perm)
      (collection-perm? user coll-path write-perm)))

(defn collection-writeable?
  "Checks to see if the suer has write permissions on coll-path. Only
   works for collection paths."
  [user coll-path]
  (collection-perm? user coll-path write-perm))

(defn owns-collection?
  "Checks to see if the user has ownership permissions on coll-path. Only
   works for collection paths."
  [user coll-path]
  (collection-perm? user coll-path own-perm))

(defn file
  [path]
  "Returns an instance of IRODSFile representing 'path'. Note that path
    can point to either a file or a directory.

    Parameters:
      path - String containing a path.

    Returns: An instance of IRODSFile representing 'path'."
  (.instanceIRODSFile (:fileFactory cm) path))

(defn exists?
  [path]
  "Returns true if 'path' exists in iRODS and false otherwise.

    Parameters:
      path - String containing a path.

    Returns: true if the path exists in iRODS and false otherwise."
  (.exists (file path)))

(defn paths-exist?
  [paths]
  "Returns true if the paths exist in iRODS.

    Parameters:
      paths - A sequence of strings containing paths.

    Returns: Boolean"
  (zero? (count (filter #(not (exists? %)) paths))))

(defn is-file?
  [path]
  "Returns true if the path is a file in iRODS, false otherwise."
  (.isFile (.instanceIRODSFile (:fileFactory cm) path)))

(defn is-dir?
  [path]
  "Returns true if the path is a directory in iRODS, false otherwise."
  (let [ff (:fileFactory cm)
        fixed-path (ft/rm-last-slash path)]
    (.isDirectory (.instanceIRODSFile ff fixed-path))))

(defn user-perms->map
  [user-perms-obj]
  (let [enum-val (.getFilePermissionEnum user-perms-obj)]
    {:user (.getUserName user-perms-obj)
     :permissions {:read  (or (= enum-val read-perm) (= enum-val own-perm))
                   :write (or (= enum-val write-perm) (= enum-val own-perm))
                   :own   (= enum-val own-perm)}}))

(defn list-user-perms
  [abs-path]
  (if (is-file? abs-path)
    (mapv
      user-perms->map
      (.listPermissionsForDataObject (:dataObjectAO cm) abs-path))
    (mapv
      user-perms->map
      (.listPermissionsForCollection (:collectionAO cm) abs-path))))

(defn list-paths
  "Returns a list of paths under the parent path. Directories end with /."
  [parent-path]
  (mapv
    #(let [full-path (ft/path-join parent-path %1)]
       (if (is-dir? full-path)
         (ft/add-trailing-slash full-path)
         full-path))
    (.getListInDir (:fileSystemAO cm) (file parent-path))))

(defn data-object
  [path]
  "Returns an instance of DataObject represeting 'path'."
  (.findByAbsolutePath (:dataObjectAO cm) path))

(defn collection
  [path]
  "Returns an instance of Collection (the Jargon version) representing
    a directory in iRODS."
  (.findByAbsolutePath (:collectionAO cm) (ft/rm-last-slash path)))

(defn lastmod-date
  [path]
  "Returns the date that the file/directory was last modified."
  (cond
    (is-dir? path)  (str (long (.getTime (.getModifiedAt (collection path)))))
    (is-file? path) (str (long (.getTime (.getUpdatedAt (data-object path)))))
    :else nil))

(defn created-date
  [path]
  "Returns the date that the file/directory was created."
  (cond
    (is-dir? path)  (str (long (.. (collection path) getCreatedAt getTime)))
    (is-file? path) (str (long (.. (data-object path) getUpdatedAt getTime)))
    :else           nil))

(defn- dir-stat
  [path]
  "Returns status information for a directory."
  (let [coll (collection path)]
    {:type     :dir
     :created  (str (long (.. coll getCreatedAt getTime)))
     :modified (str (long (.. coll getModifiedAt getTime)))}))

(defn- file-stat
  [path]
  "Returns status information for a file."
  (let [data-obj (data-object path)]
    {:type     :file
     :size     (.getDataSize data-obj)
     :created  (str (long (.. data-obj getUpdatedAt getTime)))
     :modified (str (long (.. data-obj getUpdatedAt getTime)))}))

(defn stat
  [path]
  "Returns status information for a path."
  (cond
   (is-dir? path)  (dir-stat path)
   (is-file? path) (file-stat path)
   :else           nil))

(defn file-size
  [path]
  "Returns the size of the file in bytes."
  (.getDataSize (data-object path)))

(defn response-map
  [action paths]
  {:action action :paths paths})

(defn user-exists?
  [user]
  "Returns true if 'user' exists in iRODS."
  (try
    (do 
      (.findByName (:userAO cm) user) 
      true)
    (catch java.lang.Exception d false)))

(defn set-owner
  [path owner]
  "Sets the owner of 'path' to the username 'owner'.

    Parameters:
      path - The path whose owner is being set.
      owner - The username of the user who will be the owner of 'path'."
  (if (is-file? path)
    (.setAccessPermissionOwn (:dataObjectAO cm) @zone path owner)
    (if (is-dir? path)
      (.setAccessPermissionOwn (:collectionAO cm) @zone path owner true))))

(defn set-inherits
  [path]
  "Sets the inheritance attribute of a collection to true.

    Parameters:
      path - The path being altered."
  (if (is-dir? path)
    (.setAccessPermissionInherit (:collectionAO cm) @zone path false)))

(defn is-writeable?
  [user path]
  "Returns true if 'user' can write to 'path'.

    Parameters:
      user - String containign a username.
      path - String containing an absolute path for something in iRODS."
  (cond
    (not (user-exists? user)) false
    (is-dir? path)            (collection-writeable? user (.replaceAll path "/$" ""))
    (is-file? path)           (dataobject-writeable? user (.replaceAll path "/$" ""))
    :else                     false))

(defn is-readable?
  [user path]
  "Returns true if 'user' can read 'path'.

    Parameters:
      user - String containing a username.
      path - String containing an path for something in iRODS."
  (cond
    (not (user-exists? user)) false
    (is-dir? path)            (collection-readable? user (.replaceAll path "/$" ""))
    (is-file? path)           (dataobject-readable? user (.replaceAll path "/$" ""))
    :else                     false))

(defn last-dir-in-path
  [path]
  "Returns the name of the last directory in 'path'.

    Please note that this function works by calling
    getCollectionLastPathComponent on a Collection instance and therefore
    hits iRODS every time you call it. Don't call this from within a loop.

    Parameters:
      path - String containing the path for an item in iRODS.

    Returns:
      String containing the name of the last directory in the path."
  (.getCollectionLastPathComponent 
    (.findByAbsolutePath (:collectionAO cm) (ft/rm-last-slash path))))

(defn sub-collections
  [path]
  "Returns a sequence of Collections that reside directly in the directory
    refered to by 'path'.

    Parameters:
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing Collections (the Jargon kind) representing
      directories that reside under the directory represented by 'path'."
  (.listCollectionsUnderPath (:lister cm) (ft/rm-last-slash path) 0))

(defn sub-collection-paths
  [path]
  "Returns a sequence of string containing the paths for directories
    that live under 'path' in iRODS.

    Parameters:
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing the paths for directories that live under 'path'."
  (map
    #(.getFormattedAbsolutePath %)
    (sub-collections path)))

(defn sub-dir-maps
  [user list-obj filter-files]
  (let [abs-path (.getFormattedAbsolutePath list-obj)
        basename (ft/basename abs-path)
        lister   (:lister cm)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (collection-perm-map user abs-path)
     :hasSubDirs    (pos? (count (.listCollectionsUnderPath lister abs-path 0)))
     :date-created  (str (long (.. list-obj getCreatedAt getTime)))
     :date-modified (str (long (.. list-obj getModifiedAt getTime)))}))

(defn sub-file-maps
  [user list-obj]
  (let [abs-path (.getFormattedAbsolutePath list-obj)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (dataobject-perm-map user abs-path)
     :date-created  (str (long (.. list-obj getCreatedAt getTime)))
     :date-modified (str (long (.. list-obj getModifiedAt getTime)))
     :file-size     (str (.getDataSize list-obj))}))

(defn paths-writeable?
  [user paths]
  "Returns true if all of the paths in 'paths' are writeable by 'user'.

    Parameters:
      user - A string containing the username of the user requesting the check.
      paths - A sequence of strings containing the paths to be checked."
  (reduce 
    #(and %1 %2) 
    (map 
      #(is-writeable? user %) 
      paths)))

;;Metadata

(defn map2avu
  [avu-map]
  "Converts an avu map into an AvuData instance."
  (AvuData/instance (:attr avu-map) (:value avu-map) (:unit avu-map)))

(defn get-metadata
  [dir-path]
  "Returns all of the metadata associated with a path." 
  (mapv
    #(hash-map :attr  (.getAvuAttribute %1)
               :value (.getAvuValue %1)
               :unit  (.getAvuUnit %1))
    (if (is-dir? dir-path)
      (.findMetadataValuesForCollection (:collectionAO cm) dir-path)
      (.findMetadataValuesForDataObject (:dataObjectAO cm) dir-path))))

(defn get-attribute
  [dir-path attr]
  "Returns a list of avu maps for set of attributes associated with dir-path"
  (filter
    #(= (:attr %1) attr)
    (get-metadata dir-path)))

(defn attribute?
  [dir-path attr]
  "Returns true if the path has the associated attribute."
  (pos? (count (get-attribute dir-path attr))))

(defn set-metadata
  [dir-path attr value unit]
  "Sets an avu for dir-path."
  (let [avu    (AvuData/instance attr value unit)
        ao-obj (if (is-dir? dir-path) 
                 (:collectionAO cm) 
                 (:dataObjectAO cm))]
    (if (zero? (count (get-attribute dir-path attr)))
      (.addAVUMetadata ao-obj dir-path avu)
      (let [old-avu (map2avu (first (get-attribute dir-path attr)))]
        (.modifyAVUMetadata ao-obj dir-path old-avu avu)))))

(defn delete-metadata
  [dir-path attr]
  "Deletes an avu from dir-path."
  (let [fattr  (first (get-attribute dir-path attr))
        avu    (map2avu fattr)
        ao-obj (if (is-dir? dir-path) 
                 (:collectionAO cm) 
                 (:dataObjectAO cm))]
    (.deleteAVUMetadata ao-obj dir-path avu)))

(defn list-all
  [dir-path]
  (.listDataObjectsAndCollectionsUnderPath (:lister cm) dir-path))

(defn mkdir
  [dir-path]
  (.mkdir (:fileSystemAO cm) (file dir-path) true))

(defn mkdirs
  [dir-path]
  (.mkdirs (file dir-path)))

(defn delete
  [a-path]
  (let [fileSystemAO (:fileSystemAO cm)
        resource     (file a-path)]
    (if @use-trash
      (if (is-dir? a-path)
        (.directoryDeleteNoForce fileSystemAO resource)
        (.fileDeleteNoForce fileSystemAO resource))
      (if (is-dir? a-path)
        (.directoryDeleteForce fileSystemAO resource)
        (.fileDeleteForce fileSystemAO resource)))))

(defn move
  [source dest]
  (let [fileSystemAO (:fileSystemAO cm)
        src          (file source)
        dst          (file dest)]
    (if (is-file? source)
      (.renameFile fileSystemAO src dst)
      (.renameDirectory fileSystemAO src dst))))

(defn move-all
  [sources dest]
  (mapv 
    #(move %1 (ft/path-join dest (ft/basename %1))) 
    sources))

(defn output-stream
  "Returns an FileOutputStream for a file in iRODS pointed to by 'output-path'."
  [output-path]
  (.instanceIRODSFileOutputStream (:fileFactory cm) (file output-path)))

(defn input-stream
  "Returns a FileInputStream for a file in iRODS pointed to by 'input-path'"
  [input-path]
  (.instanceIRODSFileInputStream (:fileFactory cm) (file input-path)))

(defn proxy-input-stream
  [istream cm]
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
  [fpath buffer]
  (.read (IRODSFileReader. (file fpath) (:fileFactory cm)) buffer))

(defn shopping-cart
  [filepaths]
  (let [cart (FileShoppingCart/instance)]
    (loop [fps filepaths]
      (.addAnItem cart (ShoppingCartEntry/instance (first fps)))
      (if (pos? (count (rest fps)))
        (recur (rest fps))))
    cart))

(defn temp-password
  [user]
  (.getTemporaryPasswordForASpecifiedUser (:userAO cm) user))

(defn cart-service
  []
  (ShoppingCartServiceImpl. 
    (:accessObjectFactory cm) 
    (:irodsAccount cm) 
    (DataCacheServiceFactoryImpl. (:accessObjectFactory cm))))

(defn store-cart
  [user cart-key filepaths]
  (.serializeShoppingCartAsSpecifiedUser 
    (cart-service) 
    (shopping-cart filepaths) 
    cart-key 
    user))

(defn permissions
  [user fpath]
  (cond
    (is-dir? fpath)
    (collection-perm-map user fpath)
    
    (is-file? fpath)
    (dataobject-perm-map user fpath)

    :else
    {:read false
     :write false
     :own false}))

(defn remove-permissions
  [user fpath]
  (cond
   (is-file? fpath)
   (.removeAccessPermissionsForUserInAdminMode 
     (:dataObjectAO cm) 
     (:zone cm) 
     fpath 
     user)
   
   (is-dir? fpath)
   (.removeAccessPermissionForUserAsAdmin 
     (:collectionAO cm) 
     (:zone cm) 
     fpath 
     user 
     true)))

(defn set-dataobj-perms
  [user fpath read? write? own?]
  (let [dataobj (:dataObjectAO cm)
        zone    (:zone cm)] 
    (.removeAccessPermissionsForUserInAdminMode dataobj zone fpath user)           
    (cond
      own?   (.setAccessPermissionOwnInAdminMode dataobj zone fpath user)
      write? (.setAccessPermissionWriteInAdminMode dataobj zone fpath user)
      read?  (.setAccessPermissionReadInAdminMode dataobj zone fpath user))))

(defn set-coll-perms
  [user fpath read? write? own? recursive?]
  (let [coll    (:collectionAO cm)
        zone    (:zone cm)]
    (.removeAccessPermissionForUserAsAdmin coll zone fpath user recursive?)
    
    (cond
      own?   (.setAccessPermissionOwnAsAdmin coll zone fpath user recursive?)
      write? (.setAccessPermissionWriteAsAdmin coll zone fpath user recursive?)
      read?  (.setAccessPermissionReadAsAdmin coll zone fpath user recursive?))))

(defn set-permissions
  ([user fpath read? write? own?]
     (set-permissions user fpath read? write? own? false))
  ([user fpath read? write? own? recursive?]
    (cond
      (is-file? fpath)
      (set-dataobj-perms user fpath read? write? own?)
      
      (is-dir? fpath)
      (set-coll-perms user fpath read? write? own? recursive?))))

(defn owns?
  [user fpath]
  (cond
    (is-file? fpath)
    (owns-dataobject? user fpath)
    
    (is-dir? fpath)
    (owns-collection? user fpath)
    
    :else
    false))

(defn remove-access-permissions
  [user abs-path]
  (cond
   (is-file? abs-path)
   (.removeAccessPermissionsForUserInAdminMode 
     (:dataObjectAO cm) 
     (:zone cm) 
     abs-path 
     user)

   (is-dir? abs-path)
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
  [abs-path & owners]
  (let [curr-user-perms   (list-user-perms abs-path)
        set-of-new-owners (set owners)
        rm-zone           #(if (string/split %1 #"\#")
                             (first (string/split %1 #"\#"))
                             "")]
    (doseq [non-user (filterv
                      #(not (contains? set-of-new-owners %1))
                      (map :user curr-user-perms))]
      (remove-access-permissions non-user abs-path))
    
    (doseq [new-owner set-of-new-owners]
      (set-owner abs-path new-owner))))

(defn ticket-admin-service
  "Creates an instance of TicketAdminService, which provides
   access to utility methods for performing operations on tickets.
   Probably doesn't need to be called directly."
  [user]
  (let [tsf (TicketServiceFactoryImpl. (:accessObjectFactory cm))]
    (.instanceTicketAdminService tsf (account user (temp-password user)))))

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
  [user fpath ticket-id & {:as ticket-opts}]
  (let [tas        (ticket-admin-service user)
        read-mode  TicketCreateModeEnum/READ
        new-ticket (.createTicket tas read-mode (file fpath) ticket-id)]
    (set-ticket-options ticket-id tas ticket-opts)
    new-ticket))

(defn modify-ticket
  [user ticket-id & {:as ticket-opts}]
  (set-ticket-options ticket-id (ticket-admin-service user) ticket-opts))

(defn delete-ticket
  "Deletes the ticket specified by ticket-id."
  [user ticket-id]
  (.deleteTicket (ticket-admin-service user) ticket-id))

(defn ticket?
  "Checks to see if ticket-id is already being used as a ticket
   identifier."
  [user ticket-id]
  (.isTicketInUse (ticket-admin-service user) ticket-id))

(defn ticket-by-id
  "Looks up the ticket by the provided ticket-id string and
   returns an instance of Ticket."
  [user ticket-id]
  (.getTicketForSpecifiedTicketString 
    (ticket-admin-service user) 
    ticket-id))

(defn ticket-expired?
  [ticket-obj]
  (if (.getExpireTime ticket-obj)
    (.. (java.util.Date.) (after (.getExpireTime ticket-obj)))
    false))

(defn ticket-used-up?
  [ticket-obj]
  (> (.getUsesCount ticket-obj) (.getUsesLimit ticket-obj)))

(defn init-ticket-session
  [ticket-id]
  (.. (:accessObjectFactory cm)
    getIrodsSession
    (currentConnection (:irodsAccount cm))
    (irodsFunction 
      (TicketInp/instanceForSetSessionWithTicket ticket-id))))

(defn ticket-input-stream
  [user ticket-id]
  (init-ticket-session ticket-id)
  (input-stream (.getIrodsAbsolutePath (ticket-by-id user ticket-id))))

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
  [user]
  (mapv quota-map (.listQuotaForAUser (:quotaAO cm) user)))

(defmacro with-jargon
  [& body]
  `(binding [curr-with-jargon-index (dosync (alter with-jargon-index inc))]
     (log/debug "curr-with-jargon-index:" curr-with-jargon-index)
     (let [context# (create-jargon-context-map)]
      (binding [cm context#]
        (let [retval# (do ~@body)]
          (if (instance? IRODSFileInputStream retval#)
            (do (log/debug curr-with-jargon-index "- returning a proxy input stream...")
                (proxy-input-stream retval# cm)) ;The proxied InputStream handles clean up.
            (do (log/debug curr-with-jargon-index "- cleaning up and returning a plain value")
                (clean-return retval#))))))))


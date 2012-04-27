(ns clj-jargon.jargon
  (:require [clojure-commons.file-utils :as ft])
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
           [java.io FileInputStream]))

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

;set up the thread-local var
(def ^:dynamic cm nil)

(defn init
  "Resets the connection config atoms with the values passed in."
  ([ahost aport auser apass ahome azone ares]
    (init ahost aport auser apass ahome azone ares 0 0))
  ([ahost aport auser apass ahome azone ares num-retries sleep]
    (reset! host ahost)
    (reset! port aport)
    (reset! username auser)
    (reset! password apass)
    (reset! home ahome)
    (reset! zone azone)
    (reset! defaultResource ares)
    (reset! max-retries num-retries)
    (reset! retry-sleep sleep)))

(defn clean-return
  [retval]
  (. (:fileSystem cm) close)
  retval)

(defn- context-map
  []
  (let [account     (IRODSAccount. @host (Integer/parseInt @port) @username @password @home @zone @defaultResource)
        file-system (. IRODSFileSystem instance)
        aof         (. file-system getIRODSAccessObjectFactory)
        cao         (. aof getCollectionAO account)
        dao         (. aof getDataObjectAO account)
        uao         (. aof getUserAO account)
        ugao        (. aof getUserGroupAO account)
        ff          (. file-system getIRODSFileFactory account)
        fao         (. aof getIRODSFileSystemAO account)
        lister      (. aof getCollectionAndDataObjectListAndSearchAO account)]
    {:irodsAccount        account
     :fileSystem          file-system
     :accessObjectFactory aof
     :collectionAO        cao
     :dataObjectAO        dao
     :userAO              uao
     :userGroupAO         ugao
     :fileFactory         ff
     :fileSystemAO        fao
     :lister              lister
     :home                @home
     :zone                @zone}))

(defn- get-context
  []
  (let [retval {:succeeded true :retval nil :exception nil :retry false}]
    (try
      (assoc retval :retval (context-map))
      (catch java.net.ConnectException e
        (assoc retval :exception e :succeeded false :retry true))
      (catch java.lang.Exception e
        (assoc retval :exception e :succeeded false :retry false)))))

(defn create-jargon-context-map
  []
  (loop [num-tries 0]
    (let [retval (get-context)
          error? (not (:succeeded retval))
          retry? (:retry retval)]
      (cond
        (and error? retry? (< num-tries @max-retries))
        (do (Thread/sleep @retry-sleep)(recur (+ num-tries 1)))
        
        error?
        (throw (:exception retval))
        
        :else (:retval retval)))))

(defn user-groups
  [user]
  (for [ug (. (:userGroupAO cm) findUserGroupsForUser user)]
    (. ug getUserGroupName)))

(defn user-dataobject-perms
  [user data-path]
  (let [user-groups  (user-groups user)
        zone         (:zone cm)
        dataObjectAO (:dataObjectAO cm)]
      (set (into [] 
                 (filter 
                   (fn [perm] (not= perm none-perm)) 
                   (for [username user-groups]
                     (. dataObjectAO getPermissionForDataObject data-path username zone)))))))

(defn user-collection-perms
  [user coll-path]
  (let [user-groups  (user-groups user)
        zone         (:zone cm)
        collectionAO (:collectionAO cm)]
    (set 
      (into [] 
            (filter
              (fn [perm] (not= perm none-perm))
              (for [username user-groups]
                (. collectionAO getPermissionForCollection coll-path username zone)))))))

(defn dataobject-perm-map
  [user data-path]
  (let [perms  (user-dataobject-perms user data-path)
        read   (or (contains? perms read-perm) (contains? perms own-perm))
        write  (or (contains? perms write-perm) (contains? perms own-perm))
        own    (contains? perms own-perm)]
    {:read  read
     :write write
     :own own}))

(defn collection-perm-map
  [user coll-path]
  (let [perms  (user-collection-perms user coll-path)
        read   (or (contains? perms read-perm) (contains? perms own-perm))
        write  (or (contains? perms write-perm) (contains? perms own-perm))
        own    (contains? perms own-perm)]
    {:read  read
     :write write
     :own   own}))

(defn dataobject-perm?
  [username data-path checked-perm]
  (let [perms (user-dataobject-perms username data-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn dataobject-readable?
  [user data-path]
  (dataobject-perm? user data-path read-perm))
  
(defn dataobject-writeable?
  [user data-path]
  (dataobject-perm? user data-path write-perm))

(defn owns-dataobject?
  [user data-path]
  (dataobject-perm? user data-path own-perm))

(defn collection-perm?
  [username coll-path checked-perm]
  (let [perms (user-collection-perms username coll-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn collection-readable?
  [user coll-path]
  (collection-perm? user coll-path read-perm))

(defn collection-writeable?
  [user coll-path]
  (collection-perm? user coll-path write-perm))

(defn owns-collection?
  [user coll-path]
  (collection-perm? user coll-path own-perm))

(defn file
  [path]
  "Returns an instance of IRODSFile representing 'path'. Note that path
    can point to either a file or a directory.

    Parameters:
      path - String containing a path.

    Returns: An instance of IRODSFile representing 'path'."
  (.  (:fileFactory cm) (instanceIRODSFile path)))

(defn exists?
  [path]
  "Returns true if 'path' exists in iRODS and false otherwise.

    Parameters:
      path - String containing a path.

    Returns: true if the path exists in iRODS and false otherwise."
  (.. (file path) exists))

(defn paths-exist?
  [paths]
  "Returns true if the paths exist in iRODS.

    Parameters:
      paths - A sequence of strings containing paths.

    Returns: Boolean"
  (== 0 (count (for [path paths :when (not (exists? path))] path))))

(defn is-file?
  [path]
  "Returns true if the path is a file in iRODS, false otherwise."
  (.. (. (:fileFactory cm) (instanceIRODSFile path)) isFile))

(defn is-dir?
  [path]
  "Returns true if the path is a directory in iRODS, false otherwise."
  (let [ff (:fileFactory cm)
        fixed-path (ft/rm-last-slash path)]
    (.. (. ff (instanceIRODSFile fixed-path)) isDirectory)))

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
    (into []
          (map
           user-perms->map
           (.listPermissionsForDataObject (:dataObjectAO cm) abs-path)))
    (into []
          (map
           user-perms->map
           (.listPermissionsForCollection (:collectionAO cm) abs-path)))))

(defn list-paths
  "Returns a list of paths under the parent path. Directories end with /."
  [parent-path]
  (let [fao (:fileSystemAO cm)]
    (into []
          (map
           #(let [full-path (ft/path-join parent-path %1)]
              (if (is-dir? full-path)
                (ft/add-trailing-slash full-path)
                full-path))
           (.getListInDir fao (file parent-path))))))

(defn data-object
  [path]
  "Returns an instance of DataObject represeting 'path'."
  (. (:dataObjectAO cm) findByAbsolutePath path))

(defn collection
  [path]
  "Returns an instance of Collection (the Jargon version) representing
    a directory in iRODS."
  (. (:collectionAO cm) findByAbsolutePath (ft/rm-last-slash path)))

(defn lastmod-date
  [path]
  "Returns the date that the file/directory was last modified."
  (cond
    (is-dir? path)  (str (long (. (. (collection path) getModifiedAt) getTime)))
    (is-file? path) (str (long (. (. (data-object path) getUpdatedAt) getTime)))
    :else nil))

(defn created-date
  [path]
  "Returns the date that the file/directory was created."
  (cond
    (is-dir? path)  (str (long (. (. (collection path) getCreatedAt) getTime)))
    (is-file? path) (str (long (. (. (data-object path) getUpdatedAt) getTime)))
    :else             nil))

(defn file-size
  [path]
  "Returns the size of the file in bytes."
  (. (data-object path) getDataSize))

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
    (. (:dataObjectAO cm) setAccessPermissionOwn @zone path owner)
    (if (is-dir? path)
      (. (:collectionAO cm) setAccessPermissionOwn @zone path owner true))))

(defn set-inherits
  [path]
  "Sets the inheritance attribute of a collection to true.

    Parameters:
      path - The path being altered."
  (if (is-dir? path)
    (. (:collectionAO cm) setAccessPermissionInherit @zone path false)))

(defn is-writeable?
  [user path]
  "Returns true if 'user' can write to 'path'.

    Parameters:
      user - String containign a username.
      path - String containing an absolute path for something in iRODS."
  (cond
    (not (user-exists? user)) false
    (is-dir? path)            (collection-writeable? user (. path replaceAll "/$" ""))
    (is-file? path)           (dataobject-writeable? user (. path replaceAll "/$" ""))
    :else                       false))

(defn is-readable?
  [user path]
  "Returns true if 'user' can read 'path'.

    Parameters:
      user - String containing a username.
      path - String containing an path for something in iRODS."
  (cond
    (not (user-exists? user)) false
    (is-dir? path)            (collection-readable? user (. path replaceAll "/$" ""))
    (is-file? path)           (dataobject-readable? user (. path replaceAll "/$" ""))
    :else                       false))

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
  (. (. (:collectionAO cm) findByAbsolutePath (ft/rm-last-slash path)) getCollectionLastPathComponent))

(defn sub-collections
  [path]
  "Returns a sequence of Collections that reside directly in the directory
    refered to by 'path'.

    Parameters:
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing Collections (the Jargon kind) representing
      directories that reside under the directory represented by 'path'."
  (. (:lister cm) listCollectionsUnderPath (ft/rm-last-slash path) 0))

(defn sub-collection-paths
  [path]
  "Returns a sequence of string containing the paths for directories
    that live under 'path' in iRODS.

    Parameters:
      path - String containing the path to a directory in iRODS.

    Returns:
      Sequence containing the paths for directories that live under 'path'."
  (map
    (fn [s] (. s getFormattedAbsolutePath))
    (sub-collections path)))

(defn sub-dir-maps
  [user list-obj filter-files]
  (let [abs-path (. list-obj getFormattedAbsolutePath)
        basename (ft/basename abs-path)
        lister   (:lister cm)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (collection-perm-map user abs-path)
     :hasSubDirs    (> (count (. lister listCollectionsUnderPath abs-path 0)) 0)
     :date-created  (str (long (. (. list-obj getCreatedAt) getTime)))
     :date-modified (str (long (. (. list-obj getModifiedAt) getTime)))}))

(defn sub-file-maps
  [user list-obj]
  (let [abs-path (. list-obj getFormattedAbsolutePath)]
    {:id            abs-path
     :label         (ft/basename abs-path)
     :permissions   (dataobject-perm-map user abs-path)
     :date-created  (str (long (. (. list-obj getCreatedAt) getTime)))
     :date-modified (str (long (. (. list-obj getModifiedAt) getTime)))
     :file-size     (str (. list-obj getDataSize))}))

(defn paths-writeable?
  [user paths]
  "Returns true if all of the paths in 'paths' are writeable by 'user'.

    Parameters:
      user - A string containing the username of the user requesting the check.
      paths - A sequence of strings containing the paths to be checked."
  (reduce (fn [f s] (and f s)) (map (fn [p] (is-writeable? user p)) paths)))

;;Metadata

(defn map2avu
  [avu-map]
  "Converts an avu map into an AvuData instance."
  (AvuData/instance (:attr avu-map) (:value avu-map) (:unit avu-map)))

(defn get-metadata
  [dir-path]
  "Returns all of the metadata associated with a path." 
  (map
    (fn [mv]
      {:attr  (. mv getAvuAttribute)
       :value (. mv getAvuValue)
       :unit  (. mv getAvuUnit)})
    (if (is-dir? dir-path)
      (. (:collectionAO cm) findMetadataValuesForCollection dir-path)
      (. (:dataObjectAO cm) findMetadataValuesForDataObject dir-path))))

(defn get-attribute
  [dir-path attr]
  "Returns a list of avu maps for set of attributes associated with dir-path"
  (filter
    (fn [avu-map] 
      (= (:attr avu-map) attr))
    (get-metadata dir-path)))

(defn attribute?
  [dir-path attr]
  "Returns true if the path has the associated attribute."
  (> (count (get-attribute dir-path attr)) 0))

(defn set-metadata
  [dir-path attr value unit]
  "Sets an avu for dir-path."
  (let [avu    (AvuData/instance attr value unit)
        cao    (:collectionAO cm)
        dao    (:dataObjectAO cm)
        ao-obj (if (is-dir? dir-path) cao dao)]
    (if (== 0 (count (get-attribute dir-path attr)))
      (. ao-obj addAVUMetadata dir-path avu)
      (let [old-avu (map2avu (first (get-attribute dir-path attr)))]
        (. ao-obj modifyAVUMetadata dir-path old-avu avu)))))

(defn delete-metadata
  [dir-path attr]
  "Deletes an avu from dir-path."
  (let [fattr  (first (get-attribute dir-path attr))
        avu    (map2avu fattr)
        cao    (:collectionAO cm)
        dao    (:dataObjectAO cm)
        ao-obj (if (is-dir? dir-path) cao dao)]
    (. ao-obj deleteAVUMetadata dir-path avu)))

(defn list-all
  [dir-path]
  (let [lister (:lister cm)]
    (. lister listDataObjectsAndCollectionsUnderPath dir-path)))

(defn mkdir
  [dir-path]
  (let [fileSystemAO (:fileSystemAO cm)]
    (. fileSystemAO mkdir (file dir-path) true)))

(defn mkdirs
  [dir-path]
  (. (file dir-path) mkdirs))

(defn delete
  [a-path]
  (let [fileSystemAO (:fileSystemAO cm)]
    (if (is-dir? a-path)
      (. fileSystemAO directoryDeleteForce (file a-path))
      (. fileSystemAO fileDeleteForce (file a-path)))))

(defn move
  [source dest]
  (let [fileSystemAO (:fileSystemAO cm)
        src          (file source)
        dst          (file dest)]
    (if (is-file? source)
      (. fileSystemAO renameFile src dst)
      (. fileSystemAO renameDirectory src dst))))

(defn move-all
  [sources dest]
  (into [] (map #(move %1 (ft/path-join dest (ft/basename %1))) sources)))

(defn output-stream
  "Returns an FileOutputStream for a file in iRODS pointed to by 'output-path'."
  [output-path]
  (let [fileFactory (:fileFactory cm)]
    (. fileFactory instanceIRODSFileOutputStream (file output-path))))

(defn input-stream
  "Returns a FileInputStream for a file in iRODS pointed to by 'input-path'"
  [input-path]
  (let [fileFactory (:fileFactory cm)]
    (. fileFactory instanceIRODSFileInputStream (file input-path))))

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
  (let [fileFactory (:fileFactory cm)
        read-file   (file fpath)]
    (. (IRODSFileReader. read-file fileFactory) read buffer)))

(defn shopping-cart
  [filepaths]
  (let [cart (FileShoppingCart/instance)]
    (loop [fps filepaths]
      (.addAnItem cart (ShoppingCartEntry/instance (first fps)))
      (if (> (count (rest fps)) 0)
        (recur (rest fps))))
    cart))

(defn temp-password
  [user]
  (let [uao (:userAO cm)]
    (.getTemporaryPasswordForASpecifiedUser uao user)))

(defn store-cart
  [user cart-key filepaths]
  (let [aof      (:accessObjectFactory cm)
        account  (:irodsAccount cm)
        cart-svc (ShoppingCartServiceImpl. aof account (DataCacheServiceFactoryImpl. aof))
        cart     (shopping-cart filepaths)]
    (.serializeShoppingCartAsSpecifiedUser cart-svc cart cart-key user)))

(defn permissions
  [user fpath]
  (cond
    (is-dir? fpath)
    (let [perms (user-collection-perms user fpath)]
      {:read (contains? perms read-perm)
       :write (contains? perms write-perm)
       :own (contains? perms own-perm)})
    
    (is-file? fpath)
    (let [perms (user-dataobject-perms user fpath)]
      {:read (contains? perms read-perm)
       :write (contains? perms write-perm)
       :own (contains? perms own-perm)})
    
    :else
    {:read false
     :write false
     :own false}))

(defn remove-permissions
  [user fpath]
  (cond
   (is-file? fpath)
   (let [dataobj (:dataObjectAO cm)
         zone    (:zone cm)]
     (.removeAccessPermissionsForUserInAdminMode dataobj zone fpath user))
   
   (is-dir? fpath)
   (let [coll (:collectionAO cm)
         zone (:zone cm)]
     (.removeAccessPermissionForUserAsAdmin coll zone fpath user true))))

(defn set-permissions
  [user fpath read? write? own?]
  (cond
    (is-file? fpath)
    (let [dataobj (:dataObjectAO cm)
          zone    (:zone cm)]
      (.removeAccessPermissionsForUserInAdminMode dataobj zone fpath user)
      
      (when own?
        (.setAccessPermissionOwnInAdminMode dataobj zone fpath user))
      
      (when write?
        (.setAccessPermissionWriteInAdminMode dataobj zone fpath user))
      
      (when read?
        (.setAccessPermissionReadInAdminMode dataobj zone fpath user)))
    
    (is-dir? fpath)
    (let [coll (:collectionAO cm)
          zone (:zone cm)]
      (.removeAccessPermissionForUserAsAdmin coll zone fpath user false)
      
      (when own?
        (.setAccessPermissionOwnAsAdmin coll zone fpath user false))
      
      (when write?
        (.setAccessPermissionWriteAsAdmin coll zone fpath user false))
      
      (when read?
        (.setAccessPermissionReadAsAdmin coll zone fpath user false)))))

(defn owns?
  [user fpath]
  (cond
    (is-file? fpath)
    (owns-dataobject? user fpath)
    
    (is-dir? fpath)
    (owns-collection? user fpath)
    
    :else
    false))

(defmacro with-jargon
  [& body]
  `(let [context# (create-jargon-context-map)]
     (binding [cm context#]
       (let [retval# (do ~@body)]
         (if (instance? IRODSFileInputStream retval#)
           (proxy-input-stream retval# cm) ;The proxied InputStream handles clean up.
           (clean-return retval#))))))

(ns clj-jargon.jargon
  (:require [clojure-commons.file-utils :as ft]
            [clojure.tools.logging :as log]
            [clojure.string :as string])
  (:import [org.irods.jargon.core.exception DataNotFoundException]
           [org.irods.jargon.core.protovalues FilePermissionEnum]
           [org.irods.jargon.core.pub.domain 
            AvuData
            ObjStat$SpecColType]
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
           :quotaAO             (.getQuotaAO aof acnt))))

(defn- log-value
  [msg value]
  (log/debug curr-with-jargon-index "-" msg value)
  value)

(defn- get-context
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
  "Creates a map containing instances of commonly used Jargon objects."
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

(defn user-groups
  "Returns a list of group names that the user is in."
  [cm user]
  (for [ug (.findUserGroupsForUser (:userGroupAO cm) user)]
    (.getUserGroupName ug)))

(defn user-dataobject-perms
  "Returns a set of permissions that user has for the dataobject at
   data-path. Takes into account the groups that a user is in."
  [cm user data-path]
  (let [user-grps    (conj (user-groups cm user) user)
        zone         (:zone cm)
        dataObjectAO (:dataObjectAO cm)]
    (set 
      (filterv
        #(not= %1 none-perm)
        (for [username user-grps]
          (.getPermissionForDataObject dataObjectAO data-path username zone))))))

(defn user-collection-perms
  "Returns a set of permissions that a user has for the collection at
   data-path. Takes into account the groups that a user is in. "
  [cm user coll-path]
  (let [user-grps    (conj (user-groups cm user) user)
        zone         (:zone cm)
        collectionAO (:collectionAO cm)]
    (set 
      (filterv
        #(not= %1 none-perm)
        (for [username user-grps]
          (.getPermissionForCollection collectionAO coll-path username zone))))))

(defn dataobject-perm-map
  "Uses (user-dataobject-perms) to grab the 'raw' permissions for
   the user for the dataobject at data-path, and returns a map with
   the keys :read :write and :own. The values are booleans."
  [cm user data-path]
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
  (let [perms (user-dataobject-perms cm username data-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn dataobject-readable?
  "Checks to see if the user has read permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (or (dataobject-perm? cm user data-path read-perm)
      (dataobject-perm? cm user data-path write-perm)))
  
(defn dataobject-writeable?
  "Checks to see if the user has write permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (dataobject-perm? cm user data-path write-perm))

(defn owns-dataobject?
  "Checks to see if the user has ownership permissions on data-path. Only
   works for dataobjects."
  [cm user data-path]
  (dataobject-perm? cm user data-path own-perm))

(defn collection-perm?
  "Utility function that checks to see if the user has the specified
   permission for the collection path."
  [cm username coll-path checked-perm]
  (let [perms (user-collection-perms cm username coll-path)]
    (or (contains? perms checked-perm) (contains? perms own-perm))))

(defn collection-readable?
  "Checks to see if the user has read permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (or (collection-perm? cm user coll-path read-perm)
      (collection-perm? cm user coll-path write-perm)))

(defn collection-writeable?
  "Checks to see if the suer has write permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (collection-perm? cm user coll-path write-perm))

(defn owns-collection?
  "Checks to see if the user has ownership permissions on coll-path. Only
   works for collection paths."
  [cm user coll-path]
  (collection-perm? cm user coll-path own-perm))

(defn file
  [cm path]
  "Returns an instance of IRODSFile representing 'path'. Note that path
    can point to either a file or a directory.

    Parameters:
      path - String containing a path.

    Returns: An instance of IRODSFile representing 'path'."
  (.instanceIRODSFile (:fileFactory cm) path))

(defn exists?
  [cm path]
  "Returns true if 'path' exists in iRODS and false otherwise.

    Parameters:
      path - String containing a path.

    Returns: true if the path exists in iRODS and false otherwise."
  (.exists (file cm path)))

(defn paths-exist?
  [cm paths]
  "Returns true if the paths exist in iRODS.

    Parameters:
      paths - A sequence of strings containing paths.

    Returns: Boolean"
  (zero? (count (filter #(not (exists? cm %)) paths))))

(defn is-file?
  [cm path]
  "Returns true if the path is a file in iRODS, false otherwise."
  (.isFile (.instanceIRODSFile (:fileFactory cm) path)))

(defn is-dir?
  [cm path]
  "Returns true if the path is a directory in iRODS, false otherwise."
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
  (if (is-file? cm abs-path)
    (mapv
      user-perms->map
      (.listPermissionsForDataObject (:dataObjectAO cm) abs-path))
    (mapv
      user-perms->map
      (.listPermissionsForCollection (:collectionAO cm) abs-path))))

(defn list-paths
  "Returns a list of paths for the entries under the parent path.  This is not
   recursive.  Directories end with /.

   Parameters:
     cm - The context map
     parent-path - The path of the parrent collection (directory).

   Returns:
     It returns a list path names for the entries under the parent."
  [cm parent-path]
  (mapv
    #(let [full-path (ft/path-join parent-path %1)]
       (if (is-dir? cm full-path)
         (ft/add-trailing-slash full-path)
         full-path))
    (.getListInDir (:fileSystemAO cm) (file cm parent-path))))

(defn data-object
  [cm path]
  "Returns an instance of DataObject represeting 'path'."
  (.findByAbsolutePath (:dataObjectAO cm) path))

(defn collection
  [cm path]
  "Returns an instance of Collection (the Jargon version) representing
    a directory in iRODS."
  (.findByAbsolutePath (:collectionAO cm) (ft/rm-last-slash path)))

(defn lastmod-date
  [cm path]
  "Returns the date that the file/directory was last modified."
  (cond
    (is-dir? cm path)  (str (long (.getTime (.getModifiedAt (collection cm path)))))
    (is-file? cm path) (str (long (.getTime (.getUpdatedAt (data-object cm path)))))
    :else              nil))

(defn created-date
  [cm path]
  "Returns the date that the file/directory was created."
  (cond
    (is-dir? cm path)  (str (long (.. (collection cm path) getCreatedAt getTime)))
    (is-file? cm path) (str (long (.. (data-object cm path) getUpdatedAt getTime)))
    :else              nil))

(defn- dir-stat
  [cm path]
  "Returns status information for a directory."
  (let [coll (collection cm path)]
    {:type     :dir
     :created  (str (long (.. coll getCreatedAt getTime)))
     :modified (str (long (.. coll getModifiedAt getTime)))}))

(defn- file-stat
  [cm path]
  "Returns status information for a file."
  (let [data-obj (data-object cm path)]
    {:type     :file
     :size     (.getDataSize data-obj)
     :created  (str (long (.. data-obj getUpdatedAt getTime)))
     :modified (str (long (.. data-obj getUpdatedAt getTime)))}))

(defn stat
  [cm path]
  "Returns status information for a path."
  (cond
   (is-dir? cm path)  (dir-stat cm path)
   (is-file? cm path) (file-stat cm path)
   :else              nil))

(defn file-size
  [cm path]
  "Returns the size of the file in bytes."
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
  (if (is-file? cm path)
    (.setAccessPermissionOwn (:dataObjectAO cm) (:zone cm) path owner)
    (if (is-dir? cm path)
      (.setAccessPermissionOwn (:collectionAO cm) (:zone cm) path owner true))))

(defn set-inherits
  [cm path]
  "Sets the inheritance attribute of a collection to true.

    Parameters:
      cm - The iRODS context map
      path - The path being altered."
  (if (is-dir? cm path)
    (.setAccessPermissionInherit (:collectionAO cm) (:zone cm) path false)))

(defn is-writeable?
  [cm user path]
  "Returns true if 'user' can write to 'path'.

    Parameters:
      cm - The iRODS context map
      user - String containign a username.
      path - String containing an absolute path for something in iRODS."
  (cond
    (not (user-exists? cm user)) false
    (is-dir? cm path)            (collection-writeable? cm 
                                                        user 
                                                        (ft/rm-last-slash path))
    (is-file? cm path)           (dataobject-writeable? cm 
                                                        user 
                                                        (ft/rm-last-slash path))
    :else                        false))

(defn is-readable?
  [cm user path]
  "Returns true if 'user' can read 'path'.

    Parameters:
      cm - The iRODS context map
      user - String containing a username.
      path - String containing an path for something in iRODS."
  (cond
    (not (user-exists? cm user)) false
    (is-dir? cm path)            (collection-readable? cm
                                                       user 
                                                       (ft/rm-last-slash path))
    (is-file? cm path)           (dataobject-readable? cm 
                                                       user 
                                                       (ft/rm-last-slash path))
    :else                        false))

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
  (filter
    #(= (:attr %1) attr)
    (get-metadata cm dir-path)))

(defn attribute?
  [cm dir-path attr]
  "Returns true if the path has the associated attribute."
  (pos? (count (get-attribute cm dir-path attr))))

(defn set-metadata
  [cm dir-path attr value unit]
  "Sets an avu for dir-path."
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
  (let [fattr  (first (get-attribute cm dir-path attr))
        avu    (map2avu fattr)
        ao-obj (if (is-dir? cm dir-path) 
                 (:collectionAO cm) 
                 (:dataObjectAO cm))]
    (.deleteAVUMetadata ao-obj dir-path avu)))

(defn list-all
  [cm dir-path]
  (.listDataObjectsAndCollectionsUnderPath (:lister cm) dir-path))

(defn mkdir
  [cm dir-path]
  (.mkdir (:fileSystemAO cm) (file cm dir-path) true))

(defn mkdirs
  [cm dir-path]
  (.mkdirs (file cm dir-path)))

(defn delete
  [cm a-path]
  (let [fileSystemAO (:fileSystemAO cm)
        resource     (file cm a-path)]
    (if (:use-trash cm)
      (if (is-dir? cm a-path)
        (.directoryDeleteNoForce fileSystemAO resource)
        (.fileDeleteNoForce fileSystemAO resource))
      (if (is-dir? cm a-path)
        (.directoryDeleteForce fileSystemAO resource)
        (.fileDeleteForce fileSystemAO resource)))))

(defn move
  [cm source dest]
  (let [fileSystemAO (:fileSystemAO cm)
        src          (file cm source)
        dst          (file cm dest)]
    (if (is-file? cm source)
      (.renameFile fileSystemAO src dst)
      (.renameDirectory fileSystemAO src dst))))

(defn move-all
  [cm sources dest]
  (mapv 
    #(move cm %1 (ft/path-join dest (ft/basename %1))) 
    sources))

(defn output-stream
  "Returns an FileOutputStream for a file in iRODS pointed to by 'output-path'."
  [cm output-path]
  (.instanceIRODSFileOutputStream (:fileFactory cm) (file cm output-path)))

(defn input-stream
  "Returns a FileInputStream for a file in iRODS pointed to by 'input-path'"
  [cm input-path]
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
  (.read (IRODSFileReader. (file cm fpath) (:fileFactory cm)) buffer))

(defn shopping-cart
  [filepaths]
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
  (.serializeShoppingCartAsSpecifiedUser 
    (cart-service cm) 
    (shopping-cart filepaths) 
    cart-key 
    user))

(defn permissions
  [cm user fpath]
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

(defn set-dataobj-perms
  [cm user fpath read? write? own?]
  (let [dataobj (:dataObjectAO cm)
        zone    (:zone cm)] 
    (.removeAccessPermissionsForUserInAdminMode dataobj zone fpath user)           
    (cond
      own?   (.setAccessPermissionOwnInAdminMode dataobj zone fpath user)
      write? (.setAccessPermissionWriteInAdminMode dataobj zone fpath user)
      read?  (.setAccessPermissionReadInAdminMode dataobj zone fpath user))))

(defn set-coll-perms
  [cm user fpath read? write? own? recursive?]
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
    (cond
      (is-file? cm fpath)
      (set-dataobj-perms cm user fpath read? write? own?)
      
      (is-dir? cm fpath)
      (set-coll-perms cm user fpath read? write? own? recursive?))))

(defn owns?
  [cm user fpath]
  (cond
    (is-file? cm fpath)
    (owns-dataobject? cm user fpath)
    
    (is-dir? cm fpath)
    (owns-collection? cm user fpath)
    
    :else
    false))

(defn remove-access-permissions
  [cm user abs-path]
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
  (let [dto (data-transfer-obj cm)
        res (or (:defaultResource cm) "demoResc")]
    (.copy dto source res dest nil nil)))

(defmacro with-jargon
  "An iRODS connection is opened, binding the connection's context to the 
    symbolic cm-sym value.  Next it evaluates the body expressions.  Finally, it
    closes the iRODS connection*.  The body expressions should use the value of
    cm-sym to access the iRODS context.

    Parameters:
      cfg - The Jargon configuration used to connect to iRODS.
      [cm-sym] - Holds the name of the binding to the iRODS context map used by 
        the body expressions.
      body - Zero or more expressions to be evaluated while an iRODS connection
        is open.

    Returns:
      It returns the result from evaluating the last expression in the body.*

    Example:
      (def config (init ...))

      (with-jargon config
        [ctx] 
        (list-all ctx \"/zone/home/user/\"))

    * If an IRODSFileInputStream is the result of the last body expression, the
      iRODS connection is not closed.  Instead, an special InputStream is 
      returned than when closed, closes the iRODS connection is well."
  [cfg [cm-sym] & body]
  `(binding [curr-with-jargon-index (dosync (alter with-jargon-index inc))]
     (log/debug "curr-with-jargon-index:" curr-with-jargon-index)
     (let [~cm-sym (create-jargon-context-map ~cfg)
           retval# (do ~@body)]
       (if (instance? IRODSFileInputStream retval#)
         (do (log/debug curr-with-jargon-index 
                        "- returning a proxy input stream...")
             (proxy-input-stream ~cm-sym retval#)) ;The proxied InputStream handles clean up.
         (do (log/debug curr-with-jargon-index 
                        "- cleaning up and returning a plain value")
             (clean-return ~cm-sym retval#))))))

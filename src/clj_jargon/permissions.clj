(ns clj-jargon.permissions
  (:use [clj-jargon.validations]
        [clj-jargon.gen-query]
        [clj-jargon.users]
        [clj-jargon.item-info]
        [slingshot.slingshot :only [try+ throw+]])
  (:require [clojure-commons.file-utils :as ft]
            [clj-jargon.lazy-listings :as ll]
            [clojure.tools.logging :as log]
            [clojure.string :as string])
  (:import [org.irods.jargon.core.protovalues FilePermissionEnum]
           [org.irods.jargon.core.query RodsGenQueryEnum]))

(declare permissions)

(def read-perm FilePermissionEnum/READ)
(def write-perm FilePermissionEnum/WRITE)
(def own-perm FilePermissionEnum/OWN)
(def none-perm FilePermissionEnum/NONE)

(defn- log-last
  [item]
  (log/warn "process-perms: " item)
  item)

(def perm-order-map
  (into {} (map vector [read-perm write-perm own-perm] (range))))

(defn str->perm-const
  [s]
  (FilePermissionEnum/valueOf (Integer/parseInt s)))

(defn collection-perms-rs
  [cm user coll-path]
  (execute-gen-query cm
   "select %s where %s = '%s' and %s = '%s'"
   [RodsGenQueryEnum/COL_COLL_ACCESS_TYPE
    RodsGenQueryEnum/COL_COLL_NAME
    coll-path
    RodsGenQueryEnum/COL_COLL_ACCESS_USER_NAME
    user]))

(defn dataobject-perms-rs
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

(defn perm-map-for
  [perms-code]
  (let [perms (FilePermissionEnum/valueOf (Integer/parseInt perms-code))]
    {:read  (contains? #{read-perm write-perm own-perm} perms)
     :write (contains? #{write-perm own-perm} perms)
     :own   (contains? #{own-perm} perms)}))

(defn user-collection-perms
  [cm user coll-path]
  (validate-path-lengths coll-path)
  (->> (conj (set (user-groups cm user)) user)
       (map #(collection-perms-rs cm % coll-path))
       (apply concat)
       (map #(.getColumnsAsList %))
       (map #(FilePermissionEnum/valueOf (Integer/parseInt (first %))))
       (set)))

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

(defn perm-map
  [[perm-id username]]
  (let [enum-val (FilePermissionEnum/valueOf (Integer/parseInt perm-id))]
    {:user        username
     :permissions {:read  (or (= enum-val read-perm) (= enum-val own-perm))
                   :write (or (= enum-val write-perm) (= enum-val own-perm))
                   :own   (= enum-val own-perm)}}))

(defn list-user-perms
  [cm abs-path]
  (let [path' (ft/rm-last-slash abs-path)]
    (validate-path-lengths path')
    (if (is-file? cm path')
      (mapv perm-map (ll/user-dataobject-perms cm path'))
      (mapv perm-map (ll/user-collection-perms cm path')))))

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

(defn one-user-to-rule-them-all?
  [cm user]
  (let [lister      (:lister cm)
        subdirs     (.listCollectionsUnderPathWithPermissions lister (ft/rm-last-slash (:home cm)) 0)
        accessible? (fn [u d] (some #(= (.getUserName %) u) (.getUserFilePermission d)))]
    (every? (partial accessible? user) subdirs)))

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

(defn list-paths
  "Returns a list of paths for the entries under the parent path.  This is not
   recursive .

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
       (ft/path-join parent-path %1)
       (catch Object _
         (when-not (contains? (set flags) :ignore-child-exns) (throw+))))
    (.getListInDir (:fileSystemAO cm) (file cm parent-path))))

(defn contains-accessible-obj?
  [cm user dpath]
  (log/warn "in contains-accessible-obj? - " user " " dpath)
  (let [retval (list-paths cm dpath)]
    (log/warn "results of list-paths " retval)
    (some #(is-readable? cm user %1) retval)))

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
  (let [user-hm   (ft/rm-last-slash (ft/path-join "/" (:zone cm) "home" user))
        parent    (ft/dirname path)
        base-dirs #{(ft/rm-last-slash (:home cm)) (trash-base-dir cm) user-hm}]
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

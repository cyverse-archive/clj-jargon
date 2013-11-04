(ns clj-jargon.users
  (:use [clj-jargon.validations]
        [clj-jargon.gen-query])
  (:import [org.irods.jargon.core.query RodsGenQueryEnum]))

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

(defn user-groups
  "Returns a list of group names that the user is in."
  [cm user]
  (for [ug (.findUserGroupsForUser (:userGroupAO cm) user)]
    (.getUserGroupName ug)))

(defn user-exists?
  [cm user]
  "Returns true if 'user' exists in iRODS."
  (try
    (do
      (.findByName (:userAO cm) user)
      true)
    (catch java.lang.Exception d false)))


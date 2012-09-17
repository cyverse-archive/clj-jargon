(ns clj-jargon.test.jargon
  (:use clojure.test
        clj-jargon.jargon)
  (:require [boxy.core :as bc]
            [boxy.jargon-if :as bj])
  (:import [org.irods.jargon.core.connection IRODSAccount]
           [org.irods.jargon.core.pub CollectionAO
                                      CollectionAndDataObjectListAndSearchAO
                                      DataObjectAO
                                      IRODSAccessObjectFactory
                                      IRODSFileSystemAO
                                      QuotaAO
                                      UserAO
                                      UserGroupAO]
           [org.irods.jargon.core.pub.io IRODSFileFactory]))


(defrecord ^{:private true} IRODSProxyStub [closed-ref?]  
  bj/IRODSProxy
  
  (close 
    [_]
    (reset! closed-ref? true))

  (getIRODSAccessObjectFactory 
    [_] 
    (bc/mk-mock-ao-factory nil))

  (getIRODSFileFactory 
    [_ acnt]
    (bc/->MockFileFactory nil nil)))
                                   
                                   
(deftest test-simple-init
  (let [cfg (init "host" "port" "user" "passwd" "home" "zone" "resource")]
    (is (= "host" (:host cfg)))
    (is (= "port" (:port cfg)))
    (is (= "user" (:username cfg)))
    (is (= "passwd" (:password cfg)))
    (is (= "home" (:home cfg)))
    (is (= "zone" (:zone cfg)))
    (is (= "resource" (:defaultResource cfg)))
    (is (= 0 (:max-retries cfg)))
    (is (= false (:use-trash cfg)))
    (is (= default-proxy-ctor (:proxy-ctor cfg)))))


(deftest test-init-options
  (let [test-ctor (fn [])
        cfg       (init "host" "port" "user" "passwd" "home" "zone" "resource" 
                        :max-retries 1
                        :retry-sleep 2
                        :use-trash   true
                        :proxy-ctor  test-ctor)]
    (is (= 1 (:max-retries cfg)))
    (is (= 2 (:retry-sleep cfg)))
    (is (= true (:use-trash cfg)))
    (is (= test-ctor (:proxy-ctor cfg)))))


(deftest test-with-jargon
  (let [closed?   (atom false)
        test-ctor #(->IRODSProxyStub closed?)
        cfg       (init "host" "0" "user" "passwd" "home" "zone" "resource" 
                        :proxy-ctor  test-ctor)]
    (with-jargon cfg [cm]
      (doall (map 
               #(is (= (% cfg) (% cm))) 
               (keys cfg)))
      (is (instance? IRODSAccount (:irodsAccount cm)))
      (is (instance? IRODSProxyStub (:fileSystem cm)))
      (is (instance? IRODSAccessObjectFactory (:accessObjectFactory cm)))
      (is (instance? CollectionAO (:collectionAO cm)))
      (is (instance? DataObjectAO (:dataObjectAO cm)))
      (is (instance? UserAO (:userAO cm)))
      (is (instance? UserGroupAO (:userGroupAO cm)))
      (is (instance? IRODSFileFactory (:fileFactory cm)))
      (is (instance? IRODSFileSystemAO (:fileSystemAO cm)))
      (is (instance? CollectionAndDataObjectListAndSearchAO (:lister cm)))
      (is (instance? QuotaAO (:quotaAO cm)))
      (is (not @closed?)))
    (is @closed?)))
    
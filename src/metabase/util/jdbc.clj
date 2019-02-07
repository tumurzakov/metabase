(ns metabase.util.jdbc
  (:require [clojure.string :as str]))

(defprotocol ^:private IClobToStr
  (clob->str ^String [this]
   "Convert a Postgres/H2/SQLServer JDBC Clob to a string. (If object isn't a Clob, this function returns it as-is.)"))

(extend-protocol IClobToStr
  nil     (clob->str [_]    nil)
  Object  (clob->str [this] this)

  org.postgresql.util.PGobject
  (clob->str [this] (.getValue this))

  ;; H2 + SQLServer clobs both have methods called `.getCharacterStream` that officially return a `Reader`,
  ;; but in practice I've only seen them return a `BufferedReader`. Just to be safe include a method to convert
  ;; a plain `Reader` to a `BufferedReader` so we don't get caught with our pants down
  java.io.Reader
  (clob->str [this]
    (clob->str (java.io.BufferedReader. this)))

  ;; Read all the lines for the `BufferedReader` and combine into a single `String`
  java.io.BufferedReader
  (clob->str [this]
    (with-open [_ this]
      (loop [acc []]
        (if-let [line (.readLine this)]
          (recur (conj acc line))
          (str/join "\n" acc)))))

  ;; H2 -- See also http://h2database.com/javadoc/org/h2/jdbc/JdbcClob.html
  org.h2.jdbc.JdbcClob
  (clob->str [this]
    (clob->str (.getCharacterStream this))))

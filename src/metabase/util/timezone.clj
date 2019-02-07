(ns metabase.util.timezone
  (:require [clojure.tools.logging :as log]
            [metabase.util
             [date :as du]
             [i18n :refer [trs]]])
  (:import java.util.TimeZone))

(defn- warn-on-timezone-conflict
  "Attempts to check the combination of report-timezone, jvm-timezone and data-timezone to determine of we have a
  possible conflict. If one is found, warn the user."
  [driver db ^TimeZone report-timezone ^TimeZone jvm-timezone ^TimeZone data-timezone]
  ;; No need to check this if we don't have a data-timezone
  (when (and data-timezone driver)
    (let [jvm-data-tz-conflict? (not (.hasSameRules jvm-timezone data-timezone))]
      (if ((resolve 'metabase.driver/supports?) driver :set-timezone)
        ;; This database could have a report-timezone configured, if it doesn't and the JVM and data timezones don't
        ;; match, we should suggest that the user configure a report timezone
        (when (and (not report-timezone)
                   jvm-data-tz-conflict?)
          (log/warn (str (trs "Possible timezone conflict found on database {0}." (:name db))
                         " "
                         (trs "JVM timezone is {0} and detected database timezone is {1}."
                              (.getID jvm-timezone) (.getID data-timezone))
                         " "
                         (trs "Configure a report timezone to ensure proper date and time conversions."))))
        ;; This database doesn't support a report timezone, check the JVM and data timezones, if they don't match,
        ;; warn the user
        (when jvm-data-tz-conflict?
          (log/warn (str (trs "Possible timezone conflict found on database {0}." (:name db))
                         " "
                         (trs "JVM timezone is {0} and detected database timezone is {1}."
                              (.getID jvm-timezone) (.getID data-timezone)))))))))

(defn call-with-effective-timezone
  "Invokes `f` with `*report-timezone*` and `*data-timezone*` bound for the given `db`"
  [db f]
  (let [driver    ((resolve 'metabase.driver.util/database->driver) db)
        report-tz (when-let [report-tz-id (and driver ((resolve 'metabase.driver.util/report-timezone-if-supported) driver))]
                    (du/coerce-to-timezone report-tz-id))
        data-tz   (some-> db :timezone du/coerce-to-timezone)
        jvm-tz    @du/jvm-timezone]
    (warn-on-timezone-conflict driver db report-tz jvm-tz data-tz)
    (binding [du/*report-timezone* (or report-tz jvm-tz)
              du/*data-timezone*   data-tz]
      (f))))

(defmacro with-effective-timezone
  "Runs `body` with `*report-timezone*` and `*data-timezone*` configured using the given `db`"
  [db & body]
  `(call-with-effective-timezone ~db (fn [] ~@body)))

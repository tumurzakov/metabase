(ns metabase.query-processor.middleware.bind-effective-timezone
  (:require [metabase.util.timezone :as u.timezone]))

(defn bind-effective-timezone
  "Middlware that ensures the report-timezone and data-timezone are bound based on the database being queried against"
  [qp]
  (fn [query]
    (u.timezone/with-effective-timezone (:database query)
      (qp query))))

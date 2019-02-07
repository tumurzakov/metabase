(ns metabase.util.connectivity
  (:import [java.net InetAddress InetSocketAddress Socket]))

(def ^:private host-up-timeout
  "Timeout (in ms) for checking if a host is available with `host-up?` and `host-port-up?`."
  5000)

(defn host-port-up?
  "Returns true if the port is active on a given host, false otherwise"
  [^String hostname, ^Integer port]
  (try
    (let [sock-addr (InetSocketAddress. hostname port)]
      (with-open [sock (Socket.)]
        (.connect sock sock-addr host-up-timeout)
        true))
    (catch Throwable _ false)))

(defn host-up?
  "Returns true if the host given by hostname is reachable, false otherwise "
  [^String hostname]
  (try
    (let [host-addr (InetAddress/getByName hostname)]
      (.isReachable host-addr host-up-timeout))
    (catch Throwable _ false)))

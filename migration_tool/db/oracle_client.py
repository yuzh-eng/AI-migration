import time


class OracleClient:
    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        import oracledb
        dsn = None
        host = self.config.get("host")
        port = self.config.get("port")
        service_name = self.config.get("service_name")
        sid = self.config.get("sid")
        ez = self.config.get("connect_string")
        if ez:
            dsn = ez
        elif host and port and sid:
            dsn = oracledb.makedsn(host, int(port), sid=sid)
        elif host and port and service_name:
            dsn = oracledb.makedsn(host, int(port), service_name=service_name)
        user = self.config.get("user")
        password = self.config.get("password")
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        return self.conn

    def test_connection(self):
        start = time.perf_counter()
        try:
            self.connect()
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return True, elapsed_ms, None
        except Exception as e:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return False, elapsed_ms, str(e)
        finally:
            self.close()

    def execute(self, sql: str):
        import oracledb
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        start = time.perf_counter()
        try:
            cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
            data = [dict(zip(cols, r)) for r in rows]
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return data, elapsed_ms, None
        except oracledb.Error as e:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return [], elapsed_ms, str(e)
        finally:
            cur.close()

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

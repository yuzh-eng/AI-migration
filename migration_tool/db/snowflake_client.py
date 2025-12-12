import time


class SnowflakeClient:
    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        import snowflake.connector
        self.conn = snowflake.connector.connect(
            account=self.config.get("account"),
            user=self.config.get("user"),
            password=self.config.get("password"),
            warehouse=self.config.get("warehouse"),
            database=self.config.get("database"),
            schema=self.config.get("schema"),
            role=self.config.get("role"),
        )
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
        import snowflake.connector
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        start = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description] if cur.description else []
            data = [dict(zip(cols, r)) for r in rows]
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return data, elapsed_ms, None
        except snowflake.connector.errors.Error as e:
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


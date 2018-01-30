import packages.mysql.mysql_connection as mysql_connection

class ImportedTimestamps:
    def __init__(self):
        self.mc = mysql_connection.MysqlConnection()

    def get_imported_timestamps(self):
        imported_timestamps = self.mc.run("""
            select distinct timestamp from imported_timestamps
        """)
        return [ it['timestamp'] for it in imported_timestamps ]


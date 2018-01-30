import packages.config as config 
import MySQLdb
import MySQLdb.cursors
from warnings import filterwarnings

class MysqlConnection():
    def __init__(self):
        self.config = config.Config()
        self.db = MySQLdb.connect(
            host=self.config.mysql_params['DB_HOST'], 
            db=self.config.mysql_params['DB_DATABASE'], 
            passwd=self.config.mysql_params['DB_PASSWORD'], 
            user=self.config.mysql_params['DB_USERNAME'],
            cursorclass=MySQLdb.cursors.DictCursor
        )
        self.cursor = self.db.cursor()
        filterwarnings('ignore', category = MySQLdb.Warning)

    def run(self, query_str, args=[]):
        self.cursor.execute(query_str, args)
        return self.cursor.fetchall()

    def insert(self, insert_str, args=[]):
        count = len(args)
        self.cursor.executemany(insert_str, args)
        self.db.commit()
        return

    def __del__(self):
        self.cursor.close()
        self.db.close()
import packages.mysql.mysql_connection as mysql_connection

class SubDealers:
    def __init__(self):
        self.mysql_connection = mysql_connection.MysqlConnection()
        dealer_rows = self.mysql_connection.run("""
            select * from dealers
        """)
        self.dealer_sub = {row['legacy_id'] : row['id'] for row in dealer_rows} 

    def __call__(self, rows):
        new_rows = []
        for row in rows:
            
            if row['delegate_dealer_id'] not in self.dealer_sub.keys():
                continue

            row['dealer_id'] = self.dealer_sub[row['delegate_dealer_id']]
            new_rows.append(row)

        return new_rows

    def single_row_sub(self, row):
        row['dealer_id'] = self.dealer_sub[row['delegate_dealer_id']]
        new_rows.append(row)

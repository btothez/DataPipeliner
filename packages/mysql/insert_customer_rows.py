import packages.mysql.mysql_connection as mysql_connection
import packages.libraries.helpers as helpers

class InsertCustomerRows:
    def __init__(self):
        self.helpers = helpers.Helpers()
        self.fields = [
            'full_name',
            'first_name',
            'middle_name',
            'last_name',
            'suffix',
            'confidence_score',
            'import_time'
        ]

    def __call__(self, rows, timestamp):
        """ Insert the rows(as tuples, obv.)
            and then return the inserted ids """

        self.mysql_connection = mysql_connection.MysqlConnection()
        tuple_list = [self.helpers.dict_to_tuple(row, self.fields, timestamp)
                        for row in rows]
        self.mysql_connection.insert(self.get_insert_str(), tuple_list)

        results = self.mysql_connection.run("""SELECT id FROM customers
            order by id DESC LIMIT {}""".format(len(tuple_list)))

        customer_ids = [result['id'] for result in results]
        customer_ids.reverse()

        del(self.mysql_connection)
        return customer_ids

    def get_insert_str(self):
        return """INSERT INTO customers ({}) VALUES ({})
        """.format(
            ', '.join(self.fields),
            ','.join(['%s' for field in self.fields])
            )

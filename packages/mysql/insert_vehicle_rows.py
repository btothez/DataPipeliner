import packages.mysql.mysql_connection as mysql_connection
import packages.libraries.helpers as helpers

class InsertVehicleRows:

    def __init__(self):
        self.helpers = helpers.Helpers()
        self.fields = [
            'vin',
            'year',
            'make',
            'model',
            'model_number',
            'import_time'
        ]

    def __call__(self, rows, timestamp):
        """ Insert the rows(as tuples, obv.)
            and then return the inserted ids """

        tuple_list = [self.helpers.dict_to_tuple(row, self.fields, timestamp)
                        for row in rows]

        self.mysql_connection = mysql_connection.MysqlConnection()
        self.mysql_connection.insert(self.get_insert_str(), tuple_list)
        del(self.mysql_connection)

        return

    def get_insert_str(self):
        return """INSERT INTO vehicles ({}) VALUES ({})
        """.format(
            ', '.join(self.fields),
            ','.join(['%s' for field in self.fields])
            )

import packages.mysql.mysql_connection as mysql_connection
import packages.libraries.helpers as helpers

class InsertCustomerInfoRows:
    def __init__(self):
        self.helpers = helpers.Helpers()
        self.fields = [
            'customer_id',
            'dealer_id',
            'birth_date',
            'address_line_1',
            'address_line_2',
            'city',
            'state',
            'zip',
            'county',
            'home_phone',
            'cell_phone',
            'work_phone',
            'work_extension',
            'email_1',
            'email_2',
            'email_3',
            'language',
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

    def get_insert_str(self):
        return """INSERT INTO customer_info ({}) VALUES ({})
        """.format(
            ', '.join(self.fields),
            ','.join(['%s' for field in self.fields])
            )

import packages.sql_statements as sql
import packages.tableCreateCommands as tablecreate
import packages.tableCopyCommands as tablecopy
import packages.tableQueryCommands as tablequery
import packages.helpers as helpers
import packages.dictToTuple as dict_to_tuple
import collections
import csv
import os
import sys

class Records:

    def __init__(self, tablecreate, tablequery, timestamp=0):
        self.timestamp = timestamp
        self.tablecreate = tablecreate
        self.tablequery = tablequery
        self.records_field_list = sql.getJustFields('records')
        del(self.records_field_list[0])

        self.records_reviewed = 0
        self.dealers = 0
        self.customers_reviewed = 0
        self.matches_birthday = 0
        self.matches_phone = 0
        self.matches_email = 0
        self.matches_address = 0
        self.new_customers = 0
        self.vehicles_reviewed = 0
        self.new_vehicles = 0
        self.geolocation_data = 0
        self.manually_deduped = 0
        self.import_time = timestamp

    def create_table(self):
        self.tablecreate.create('records')

    def get_record_tuple(self):
        return (
            self.records_reviewed,
            self.dealers,
            self.customers_reviewed,
            self.matches_birthday,
            self.matches_phone,
            self.matches_email,
            self.matches_address,
            self.new_customers,
            self.vehicles_reviewed,
            self.new_vehicles,
            self.geolocation_data,
            self.manually_deduped,
            self.import_time
        )


    def save_record(self):

        records_list_template = self.get_record_tuple()

        insert_query = "INSERT INTO records ({0}) VALUES {1}".format(
            ', '.join(self.records_field_list), 
            records_list_template
        )
        print(insert_query)
        self.tablequery.insert(insert_query, [records_list_template], True)


import packages.sql_statements as sql_statements
import packages.tableCreateCommands as tablecreate
import packages.tableCopyCommands as tablecopy
import packages.tableQueryCommands as tablequery
#import packages.helpers as helpers
import packages.customers as customers
import packages.vehicles as vehicles
import packages.dictToTuple as dict_to_tuple
import packages.s3_wrapper as s3

import collections
import csv
import os

"""
    CREATE AND FILL PIVOT TABLES
    pivotizer.createTables()
    pivotizer.copyToTables()
"""

class Pivotizer:
    def __init__(self, tablecreate, tablecopy, tablequery, records, timestamp=0):
        self.timestamp = timestamp
        self.tablecreate = tablecreate
        self.tablecopy = tablecopy
        self.tablequery = tablequery
        self.records = records
        self.table_names = ['pivot_sales', 'pivot_service']
        # self.customers = customers.Customers(self.tablecreate, self.tablequery)
        self.customers = customers.Customers(
            self.tablecreate, self.tablequery, self.records)
        self.customers.grabAllCustomersInDefaultDict()
        self.vehicles = vehicles.Vehicles(
            self.tablecreate, self.tablequery, self.records)
        self.vehicles.grabAllVehiclesInDefaultDict()    
        self.dict_to_tuple = dict_to_tuple.DictToTuple(self.timestamp)
        self.s3 = s3.S3(bucket_name='materialized-upload')

    def createTables(self):
        """ Forge and create the tables """
        for table in self.table_names:
            self.tablecreate.create(table)

    def copyToTables(self):
        """ Foreach type of pivot: Query, write and copy """
        for table_name in self.table_names:
            table_type = table_name.replace('pivot_', '')

            query_label = "get_all_{}_rows".format(table_type)
            if self.timestamp != 0:
                query_label += '_timestamp'

            file_name = table_name + '.tsv'
            mat_list = self.tablequery.query(query_label, [], False, {'timestamp' : self.timestamp})            

            fields_list = sql_statements.getJustFields(table_name)
            fields_list.remove('id')
            
            # Define this field list for turning dict into tuple later            
            self.dict_to_tuple.defineFieldList(fields_list)
            print("Got rows")
            print("len : {}".format(len(mat_list)))
            mat_list =  list(map(self.dict_to_tuple.transform, mat_list))
            
            self.writeToFile(file_name, mat_list, table_type)
            self.moveFileToS3(file_name, table_type)
            os.remove(file_name)

        for table_name in self.table_names:
            print("Now copying {} to redshift".format(table_name))
            self.copyPivotData(table_name)
            
    def writeToFile(self, file_name, mat_list, table_type):
        """ Write to a local tsv file """
        print("File name  {}".format(file_name))
        with open(file_name, 'w') as out:
            csv_out=csv.writer(out, delimiter='\t')
            for row in mat_list:
                csv_out.writerow(row)

    def moveFileToS3(self, file_name, table_type):
        """ Upload file to S3 """
        self.s3.setKey("pivot_{}".format(table_type) + "/{}/".format(self.timestamp) + file_name)
        self.s3.copyFilesToS3(file_name)

    def copyPivotData(self, table_name):
        """ Now copy to Redshift from S3 """
        s3_location = "s3://{}/{}/{}".format(self.s3.bucket_name,
                                        table_name,
                                        self.timestamp)            
        self.tablecopy.copy(table_name, s3_location)            
    

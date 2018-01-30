import packages.config as config
import packages.libraries.helpers as helpers
import packages.mysql.insert_vehicle_rows as insert_vehicle_rows

""" 
    Insert the vehicles into the DB
"""

class VehicleInsertion:

    def __init__(self):
        self.config = config.Config()
        self.helpers = helpers.Helpers()
        self.insert_vehicle_rows = insert_vehicle_rows.InsertVehicleRows()


    def insert(self, vehicles_list, timestamp):
        chunks = self.helpers.chunkIt(vehicles_list)

        ind = 0
        for chunk in chunks:
            print("CHUNK {0} of {1}".format(ind, len(chunks)))            
            self.insert_vehicle_rows(chunk, timestamp)
            ind += 1



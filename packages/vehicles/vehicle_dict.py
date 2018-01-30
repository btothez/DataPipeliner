import packages.mysql.grab_current_vehicles as grab_current_vehicles
import collections
import sys

class VehicleDict:
    def __init__(self):
        self.gcv = grab_current_vehicles.GrabCurrentVehicles()
        self.vehicles_dict = collections.defaultdict(dict)
        self.collect()
        self.form()

    def collect(self):
        """ Get all vehicles
            from the DB """
        self.vehicles_rows = self.gcv.run()

    def form(self):
        """ Put custmoers into dict indexed by pureName.
            further indexed by dealer_id 
            and then list of customer_info """

        for row in self.vehicles_rows:
            self.vehicles_dict[row['vin']] = row['id']

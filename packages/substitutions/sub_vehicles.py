import packages.vehicles.vehicle_dict as vehicle_dict
import sys

class SubVehicles:

    def __init__(self):

        self.vd = vehicle_dict.VehicleDict()

    def __call__(self, rows):
        new_rows = []
        for row in rows:
            
            if row['vin'] not in self.vd.vehicles_dict.keys():
                print('vin not found in row : {}'.format(row['vin']))
                sys.exit()

            row['vehicle_id'] = self.vd.vehicles_dict[row['vin']]
            new_rows.append(row)

        return new_rows
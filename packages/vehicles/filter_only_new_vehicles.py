class FilterOnlyNewVehicles:

    def __init__(self, vehicles_dict):
        self.vehicles_dict = vehicles_dict

    def __call__(self, vehicles_list):
        new_list = [row for row in vehicles_list 
                    if row['vin'] not in 
                    self.vehicles_dict.keys()]

        return new_list

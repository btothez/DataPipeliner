class DedupeVehicles:

    def __init__(self):
        self.deduped_vehicles_list = []

    def __call__(self, rows):
        indexed_rows = {row['vin'] : row for row in rows}
        self.deduped_vehicles_list = list(indexed_rows.values())
        return self.deduped_vehicles_list

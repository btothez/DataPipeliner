import packages.libraries.helpers as helpers
import csv

class ServiceRowsToCsv:
    def __init__(self):
        self.helpers = helpers.Helpers()
        self.fields = [
            'id',
            'legacy_id',
            'customer_id',
            'customer_info_id',
            'vehicle_id',
            'dealer_id',
            'file_type',
            'ro_number',
            'ro_mileage',
            'operation_sale_types',
            'open_date',
            'warranty_labor_cost',
            'customer_total_cost',
            'warranty_total_sale',
            'customer_total_sale',
            'earned',
            'import_time',
            'created_at',
            'updated_at'
        ]

        self.first_time = True

    def __call__(self, rows, filename, timestamp):
        row_tups = [ self.helpers.dict_to_tuple_csv(
                        row, 
                        self.fields, 
                        timestamp )
                    for row in rows ]

        # Write header if first call
        if self.first_time:
            print('writing headers')
            self.first_time = False

            with open(filename, 'w') as handle:
                csv_out=csv.writer(handle, delimiter=',', lineterminator='\n')
                csv_out.writerow(self.fields)

        with open(filename, 'a') as handle:
            print('writing rows')
            csv_out=csv.writer(handle, delimiter=',', lineterminator='\n')
            for row in row_tups:
                csv_out.writerow(row)

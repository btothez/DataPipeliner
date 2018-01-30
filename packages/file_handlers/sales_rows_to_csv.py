import packages.libraries.helpers as helpers
import csv

class SalesRowsToCsv:
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
            'deal_type',
            'new_used',
            'deal_number',
            'mileage',
            'sales_price',
            'deal_status',
            'contract_date',
            'back_gross',
            'front_gross',
            'total_fee_aftermarket_sale',
            'warranty_5_sale',
            'warranty_4_sale',
            'warranty_3_sale',
            'warranty_2_sale',
            'warranty_1_sale',
            'finance_profit',
            'total_front_sales',
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
            print('writing to a file : {}'.format(filename))
            csv_out=csv.writer(handle, delimiter=',', lineterminator='\n')
            for row in row_tups:
                csv_out.writerow(row)

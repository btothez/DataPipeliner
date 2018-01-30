import packages.dealers.dealer_set as dealer_set
import packages.customers.objects.customer_info as customer_info

class CustomerObject:

    def __init__(self, customer_id, row):
        self.id = customer_id
        self.dealer_set = dealer_set.DealerSet(
                row['dealer_id'],
                customer_info.CustomerInfo(row)
            )

        self.dict = row

    def get_all_dealers(self):
        return self.dealer_set.keys()



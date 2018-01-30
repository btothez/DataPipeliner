import packages.mysql.grab_current_customers as grab_current_customers
import packages.customers.methods.purify_name as purify_name
import packages.customers.objects.customer_info as customer_info
import packages.customers.objects.customer_object as customer_object
import collections
import sys

class CustomerDict:
    def __init__(self):
        self.gcc = grab_current_customers.GrabCurrentCustomers()
        self.purify_name = purify_name.PurifyName()
        self.customers_dict = collections.defaultdict(dict)
        self.collect()
        self.form()

    def collect(self):
        """ Get all customers + pivot + customer_info
            from the DB """
        self.customer_rows = self.gcc.run()

    def form(self):
        """ Put custmoers into dict indexed by pureName.
            further indexed by dealer_id 
            and then list of customer_info """

        for row in self.customer_rows:

            pure_name = self.purify_name(row['full_name'])

            if pure_name not in self.customers_dict:
                self.customers_dict[pure_name] = {} 

            if row['customer_id'] not in self.customers_dict[pure_name]:
                self.customers_dict[pure_name][row['customer_id']] = \
                    customer_object.CustomerObject(row['customer_id'], row)

            else:
                self.customers_dict[pure_name][row['customer_id']].dealer_set.add_customer_info(
                    row['dealer_id'],
                    customer_info.CustomerInfo(row)
                )

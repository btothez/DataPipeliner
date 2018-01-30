import packages.customers.objects.customer_dict as customer_dict
import packages.customers.objects.customer_info as customer_info
import packages.customers.objects.customer_object as customer_object
import packages.customers.methods.purify_name as purify_name
import packages.customers.methods.customer_comparison as customer_comparison
import packages.mysql.mysql_connection as mysql_connection
import sys

class SubCustomers:

    def __init__(self):
        self.mysql_connection = mysql_connection.MysqlConnection()
        self.cd = customer_dict.CustomerDict()
        self.pn = purify_name.PurifyName()
        self.cc = customer_comparison.CustomerComparison()
        dealer_rows = self.mysql_connection.run("""
            select * from dealers
        """)
        self.dealer_sub = {row['legacy_id'] : row['id'] for row in dealer_rows} 

    def __call__(self, rows):
        new_rows = []

        for row in rows:
            customer_info_obj = self.match_customer_with_raw_row(row)    

            if not customer_info_obj:
                print("Could not get a customer_info_obj from this row")
                print(row)
                sys.exit()
            
            row['customer_info_id'] = customer_info_obj.getCustomerInfoId()
            row['customer_id'] = customer_info_obj.getCustomerId()
            new_rows.append(row)

        return new_rows


    def match_customer_with_raw_row(self, row):
        """ Return a customer for a dict-row with personal info """

        original_row = row
        row = customer_info.CustomerInfo(row)
        
        # # Get all the individuals with this name
        pure_name = self.pn(original_row['full_name'])
        # pureName = self.customers.purifyFullName(original_row['full_name'])
        dealer_id = self.dealer_sub[original_row['delegate_dealer_id']]

        if pure_name not in self.cd.customers_dict:
            print(original_row)
            print(pure_name)
            print("NAME NOT FOUND")
            # return None

        potential_customers = list(
            self.cd.customers_dict[pure_name].values()
        )

        # Only keep customers who have info for that dealer
        potential_customers = list(
            filter(
                lambda pc: dealer_id in pc.get_all_dealers(),
                potential_customers
            )
        )

        # None, that's a problem
        if len(potential_customers) == 0:
            print(original_row)
            print(pure_name)
            print(original_row['delegate_dealer_id'])
            print("DEALER NOT FOUND")
            sys.exit()
            # return None

        # Find and return the best cust_info obj
        else:
            customer_obj = None
            min_match = 1
            for pot_cust in potential_customers:
                for inf_obj in pot_cust.dealer_set.get_customer_obj_list(dealer_id):

                    match_score = self.cc.matchRows(
                        row, 
                        inf_obj.getCustomerDict()
                    )

                    if match_score < min_match:
                        customer_obj = inf_obj
                        min_match = match_score

            return customer_obj

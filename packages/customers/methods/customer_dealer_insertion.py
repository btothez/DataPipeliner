import packages.config as config
import packages.libraries.helpers as helpers
import packages.merges.merged_customers as merged_customers
import packages.mysql.insert_customer_rows as insert_customer_rows
import packages.mysql.insert_customer_info_rows as insert_customer_info_rows

"""
    This class handles inserting a single customer dict
    We insert the customer, the customer_info, and finally the pivot between the two
"""

class CustomerDealerInsertion:

    def __init__(self):
        self.config = config.Config()
        self.helpers = helpers.Helpers()

        self.merged_customers = merged_customers.MergedCustomers()
        self.merged_dict = self.merged_customers.get_merged_dict()

        # Clean up
        del(self.merged_customers)
        self.insert_customer_rows = insert_customer_rows.InsertCustomerRows()
        self.insert_customer_info_rows = insert_customer_info_rows.InsertCustomerInfoRows()

    def insert(self, customer_list, timestamp):
        """ Break the customers list into chunks,
            Insert the chunk of customers, save ids,
            insert their infos, save ids, and then insert pivot """

        self.timestamp = timestamp
        chunks = self.helpers.chunkIt(customer_list)

        ind = 0
        for chunk in chunks:
            print("CHUNK {0} of {1}".format(ind, len(chunks)))
            ind += 1

            # Split this chunk into customers to be added,
            # And customers who have already been matched
            self.insertNewCustomers([cust for cust in chunk
                                    if 'new_customer' in cust
                                    and cust['new_customer']])
            self.insertMatchedCustomers([cust for cust in chunk
                                    if 'new_customer' not in cust
                                    or cust['new_customer'] == False])

    def insertNewCustomers(self, chunk):
        """ Take a list of customers, insert them,
            And then insert all their customer_infos """

        customer_ids = self.insert_customer_rows(chunk, self.timestamp)

        # Insert those ids into the customer dictionaries
        # Also, get a big list of customer_info_obj's in the right order
        customer_info_objs_list = []

        for customer_index in range(len(chunk)):

            customer_id = customer_ids[customer_index]
            chunk[customer_index]['id'] = customer_id

            for dealer in chunk[customer_index]['dealer_set'].keys():

                cust_info_list = chunk[customer_index]['dealer_set']\
                                    .get_customer_obj_list(dealer)

                for ci in cust_info_list:
                    ci.setCustomerId(customer_id)

                customer_info_objs_list += cust_info_list

        # Now insert all of these customer_info_objs
        customer_info_rows = [obj.getCustomerDict()
                            for obj in customer_info_objs_list]

        self.insert_customer_info_rows(customer_info_rows, self.timestamp)

    def insertMatchedCustomers(self, chunk):
        """ There's no more pivot table!! So just insert
            the customer_info/dealer pair for each new
            customer_info object """

        # Insert those ids into the customer dictionaries
        # Also, get a big list of customer_info_obj's in whatever order
        customer_info_objs_list = []

        for customer in chunk:
            for delegate_dealer_id, customer_info_obj in customer['dealer_set'].get_dealer_obj_tuples():

                if customer_info_obj.getAddThis():

                    # First, Replace the customer_id, if it has been merged
                    customer_id = customer_info_obj.getCustomerId()

                    if customer_id in self.merged_dict:
                        new_customer_id = self.merged_dict[customer_id]
                        customer_info_obj.setCustomerId(new_customer_id)

                    # Now append
                    customer_info_objs_list.append(customer_info_obj)


        # Now insert all of these customer_info_objs
        customer_info_rows = [obj.getCustomerDict()
                    for obj in customer_info_objs_list]

        self.insert_customer_info_rows(customer_info_rows, self.timestamp)

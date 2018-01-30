import packages.customers.methods.purify_name as purify_name
import packages.dealers.dealer_set as dealer_set
import packages.dealers.methods.dealer_set_comparison as dealer_set_comparison

""" 
    Go through each row in deduped customers list

    For each row, is that a new customer?
    If so, add all dealers in the dealer_set, 
        and the necessary customer_info_objs

    If it's able to be matched to another customer, ie not new
        then add the new dealers
        and the necessary customer_info_objs """


class FilterOnlyNew:
    def __init__(self, customers_dict):
        self.purify_name = purify_name.PurifyName()
        self.dealer_set_comparison = dealer_set_comparison.DealerSetComparison()
        self.customers_dict = customers_dict

    def __call__(self, customers_list):
        """ Go through each row, decided if it needs to be added to added
            to the final list if there's anything new about this customer """

        new_list = []
        scores = []

        for row in customers_list:
            result = self.judge_row(row)

            if result is not None:
                new_list.append(result)

        # self.count_new_customers(new_list)
        return new_list 

    def judge_row(self, row):
        """ For this row, is this customer already in the customersDict
            are all the dealers in the dealer_set represented?
            Do we need to add customer_info's for that dealer? """

        pure_name = self.purify_name(row['full_name'])

        dealer_set = row['dealer_set']

        # If name is not in customersDict, this row must be new
        if pure_name not in self.customers_dict:
            row['new_customer'] = True
            return row

        all_customers_for_name = self.customers_dict[pure_name]

        # For each of these customers, are any a match
        match_id = 0

        for cust_id in all_customers_for_name.keys():

            cust = all_customers_for_name[cust_id]

            (best_match, 
            best_score, 
            match_dealer) = self.dealer_set_comparison.getBestMatchFromDealerSet(
                                cust.dealer_set,
                                row['dealer_set']
                            )

            # Was there any match?
            if best_match is not None:
                # print('best match is not none -- new_customer = false')
                row['new_customer'] = False
                match_id = cust_id

        # No matches, this is a new customer
        if match_id == 0:
            row['new_customer'] = True
            return row

        cust = all_customers_for_name[match_id]
        row['customer_id'] = match_id

        return self.determine_what_to_add(cust, row)

    def determine_what_to_add(self, existing_customer, row):
        """ First, add any new dealers to the row's dealer_set's
            list of new_dealers, then go through ALL customer_info_objs
            and determine to "add_this" or not
            We update self.customersDict as we go along """

        existing_dealer_set = existing_customer.dealer_set

        row_dealers = row['dealer_set'].keys()
        existing_dealers = existing_dealer_set.keys()

        for dealer in row_dealers:

            if dealer not in existing_dealers:
                row['dealer_set'].new_dealers.append(dealer)
                existing_info_objs = []

            else:
                existing_info_objs = existing_dealer_set.get_customer_obj_list(dealer)

            for cust_info in row['dealer_set'].get_customer_obj_list(dealer):

                cust_info.setCustomerId(existing_customer.id)

                if cust_info.hasNewInfo(existing_info_objs):

                    cust_info.setAddThis()
                    
                    existing_dealer_set.add_customer_info(dealer, cust_info)

        return row



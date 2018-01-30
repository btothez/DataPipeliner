import packages.customers.methods.purify_name as purify_name
import collections

class SortedRowDict:
    def __init__(self):
        self.row_dict = collections.defaultdict(list)
        self.purify_name = purify_name.PurifyName()

    def __call__(self, customers_single_list):
        """ Index rows by pure name """
        for uniq_row in customers_single_list:
            self.row_dict[ 
                    self.purify_name(uniq_row['full_name']) 
                ].append(uniq_row)

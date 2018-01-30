import packages.import_handlers.methods.uniquify_list as uniquify_list
import packages.import_handlers.methods.sorted_row_dict as sorted_row_dict
import packages.import_handlers.methods.individuals as individuals
import packages.substitutions.sub_dealers as sub_dealers

class Deduper:
    def __init__(self):
        self.rows = []
        self.deduped_customers_list = []
        self.uniquify_list = uniquify_list.UniquifyList()
        self.sorted_row_dict = sorted_row_dict.SortedRowDict()
        self.individuals = individuals.Individuals()
        self.sub_dealers = sub_dealers.SubDealers()


    def __call__(self, rows):
        """ Create unique customers from big sales &
            service query list 
            fill self.deduped_customers_list """

        print('in Deduper - adding dealer_ids')
        self.rows = self.sub_dealers(rows)

        # Store Uniq list in uniquify_list obj
        self.uniquify_list(self.rows)

        # Separate Rows into a dict
        self.sorted_row_dict(self.uniquify_list.customers_single_list)

        # Get all Uniq full names
        self.uniq_full_names = list(self.sorted_row_dict.row_dict.keys())

        # self.get_all_dealers()

        for full_name in self.uniq_full_names:

            these_rows = self.sorted_row_dict.row_dict[full_name]

            # If no rows, forget this name
            if len(these_rows) == 0:
                continue

            individuals, first_scores = self.individuals.get_individuals(these_rows)
            individuals, second_scores = self.individuals.reduce_individuals(individuals)

            self.deduped_customers_list += individuals

        return self.deduped_customers_list


# self.records.customers_reviewed = len(self.deduped_customers_list)
# def get_all_dealers(self):

#     all_dealers = frozenset(list(map(
#             lambda r: r['delegate_dealer_id'],
#             self.customersList
#         )))

# self.records.dealers = len(all_dealers)
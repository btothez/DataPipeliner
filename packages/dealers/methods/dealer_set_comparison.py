import packages.customers.methods.customer_comparison as customer_comparison
import itertools


""" 
Functions for comparing two dealersets
"""

class DealerSetComparison:
    def __init__(self):
        self.customer_comparison = customer_comparison.CustomerComparison()

    def getBestMatchFromDealerSet(self, first_row_dealer_set, dealer_set):
        any_matches = False
        best_match_score = 1
        best_match = None
        match_dealer = None

        fr_tups = first_row_dealer_set.get_dealer_obj_tuples()
        ir_tups = dealer_set.get_dealer_obj_tuples()

        """ This looks like this:
            [( (fr_dealer_1, fr_ci_1),(ir_dealer_1, ir_ci_1) ) , ((), ()), ....] """
            
        all_combos = list(itertools.product(fr_tups, ir_tups))
        for fr_tup, ir_tup in all_combos:

            fr_dealer, fr_customer_info = fr_tup
            ir_dealer, ir_customer_info = ir_tup

            # Remember: high matchScore (1) means they are different people
            match_score = self.customer_comparison.matchRows(
                fr_customer_info, 
                ir_customer_info
            )

            if match_score < 0.5 and match_score < best_match_score:
                any_matches = True
                best_match_score = match_score
                best_match = fr_customer_info
                match_dealer = ir_dealer

        """ Returning this, remember:   
            best_match - the customer_info from the first one
            match_dealer - the dealer from the indiv """
                
        return best_match, best_match_score, match_dealer

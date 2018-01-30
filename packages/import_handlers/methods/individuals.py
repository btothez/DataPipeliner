import packages.dealers.methods.dealer_set_comparison as dealer_set_comparison
import packages.dealers.dealer_set as dealer_set
import packages.customers.objects.customer_info as customer_info

class Individuals:

    def __init__(self):
        self.dealer_set_comparison = dealer_set_comparison.DealerSetComparison()

    def get_individuals(self, rows):
        """ Pop off first row, compare rest against it """

        individuals = []
        scores = []
        firstRow = rows.pop(0)
        firstRow['confidence_score'] = 1

        firstRow['dealer_set'] = dealer_set.DealerSet(
                                    # firstRow['delegate_dealer_id'], 
                                    firstRow['dealer_id'], 
                                    customer_info.CustomerInfo(firstRow)
                                )

        for row in rows:
            row['confidence_score'] = 0
            row['dealer_set'] = dealer_set.DealerSet(
                                        # row['delegate_dealer_id'], 
                                        row['dealer_id'], 
                                        customer_info.CustomerInfo(row)
                                    )

            # Compare this row to each of the ones in the first row's dealer_set
            best_match, best_score, match_dealer = self.dealer_set_comparison.getBestMatchFromDealerSet(
                                                        firstRow['dealer_set'],
                                                        row['dealer_set'] 
                                                    )

            # Remember: high matchScore (1) means they are different people
            if best_match is not None:
                # We found a match
                scores.append(best_score)
                row['confidence_score'] = 1 - best_score
                firstRow['dealer_set'].incorporate_dealer_set(row['dealer_set'])

            else:
                # Otherwise, this new row is an individual
                individuals.append(row)

        # Finally, append the first row
        individuals.append(firstRow)
        return individuals, scores

    def reduce_individuals(self, individuals):
        """ Go through list of individuals and reduce the synonyms """

        scores = []
        reduced_individuals = []


        if len(individuals) > 1:
            individual = individuals.pop()

            # for individual in individuals:
            while individual:

                # Check against all the others and remove them
                i = 0
                remove_indexes = []

                for synonym in individuals:

                    best_match, best_score, match_dealer = \
                        self.dealer_set_comparison.getBestMatchFromDealerSet(
                            individual['dealer_set'],
                            synonym['dealer_set'] 
                        )

                    if best_match is not None:
                        # Only remove it if it's not the original, 
                        # if synonym != individual:
                        remove_indexes.append(i)

                        individual['dealer_set'].incorporate_dealer_set(
                                synonym['dealer_set']
                            )

                    i += 1

                for i in sorted(remove_indexes, reverse=True):
                    # print('index: {}'.format(i))
                    del(individuals[i])

                # Add to dedupedList
                reduced_individuals.append(individual)

                individual = individuals.pop() if len(individuals) > 0 else None

            return reduced_individuals, scores

        elif len(individuals) == 1:
            return individuals, scores


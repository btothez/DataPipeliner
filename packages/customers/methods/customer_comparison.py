import math

class CustomerComparison:
    def __init__(self, records={}):
        self.records = records
        self.weightsDict = {
            'full_name' : 0,
            'salutation' : 0,
            'first_name' : 5,
            'middle_name' : 3,
            'middle_initial' : 2,
            'last_name' : 0,
            'suffix' : 0,
            'address_line_1' : 3,
            'address_line_2' : 0,
            'city' : 3,
            'state' : 4,
            'zip' : 0,
            'county' : 0,
            'home_phone' : 2,
            'cell_phone' : 2,
            'work_phone' : 0,
            'work_extension' : 0,
            'email_1' : 3,
            'email_2' : 0,
            'email_3' : 0,
            'birth_date' : 5,
            'delegate_dealer_id' : 0,
            'language,' : 0,
            'contract_date' : 0,
            'open_date' : 0,
            'confidence_score' : 0 }

    def mySig(self, x):
        x = (x * 2) - 1
        return 1 / (1 + math.exp(-x * 3))

    def checkAprioriMatch(self, rowA, rowB):
        """ Check for certain fields that mean the 
            individual rows are definitely the same person """ 
            
        # Same Birthday
        if rowA['birth_date'] == rowB['birth_date'] and rowA['birth_date'] and rowB['birth_date']:
            # self.records.matches_birthday += 1
            return True

        # Same telephone numbers
        if len(set(rowA['numbers']).intersection(set(rowB['numbers']))) > 0:
            # self.records.matches_phone += 1
            return True

        # Same emails
        if len(set(rowA['emails']).intersection(set(rowB['emails']))) > 0:
            # self.records.matches_email += 1
            return True

        return False

    def matchRows(self, A, B):
        """ Compare a score comparing two rows:
            High score (1) if different people,
            Low score (0) if same person """

        # Can take two rows or two customer_inf objs
        rowA = A
        rowB = B
        
        if type(A) is not dict:
            rowA = A.getCustomerDict()

        if type(B) is not dict:
            rowB = B.getCustomerDict()

        apriori_match = self.checkAprioriMatch(rowA, rowB) 
        if apriori_match:
            return 0

        comparison = self.compareRows(rowA, rowB)
        difference = comparison['difference']


        composite = comparison['composite']
        nonTrivialDiffernce = list(filter(lambda key: not any(
            [triv == key for triv in ['open_date', 
                                      'contract_date', 
                                      'confidence_score']
            ]), difference.keys()))

        if len(nonTrivialDiffernce) > 0:

            # Mostly, let's score the difference between these two
            matchScore = self.scoreMatch(difference, composite, 
                                         nonTrivialDiffernce)        
            
            # if matchScore < 0.5:
            #     self.records.matches_address += 1

            return matchScore

        else:
            return 0        

    def scoreMatch(self, difference, composite, nonTrivialDiffernce):
        """ scoreMatch:
            Retrun a score for the differences b/w these rows
            This will be close to 1 if they are very different
            and close to 0 if they are almost identical """

        diffKeys = difference.keys()

        # Make Identity Vector differences
        idenVec = [1 if field in difference else 0 for field in self.weightsDict]

        # print()
        # print("DIFF")
        # print(difference)
        # print()

        # Make weights array of differences
        weightVec = [self.weightsDict[field] for field in self.weightsDict]        
        productVec = [a*b for a,b in zip(weightVec, idenVec)]
        potentialTotal = sum(weightVec)

        # print()
        # print("potentialTotal")
        # print(potentialTotal)
        # print()

        total = sum(productVec)

        # print()
        # print("total")
        # print(total)
        # print()

        ratio = total / float(potentialTotal)
        
        # print()
        # print("ratio")
        # print(ratio)
        # print()

        # print()
        # print("sig ratio")
        # print(self.mySig(ratio))
        # print()

        return self.mySig(ratio)
    

    def compareRows(self, rowA, rowB):
        """ If the two dicts are identical - return true
            If one has fields that are empty (or null) in the other, return the union
            If some fields differ, return those differences """

        composite = {}
        difference = {} 
        allKeys = list(rowA.keys() | rowB.keys())
        for field in allKeys:
            a = rowA.get(field, None)
            b = rowB.get(field, None)

            if a is None:
                composite[field] = b
            elif b is None:
                composite[field] = a
            elif (a == b):
                composite[field] = a
            else:
                difference[field] = (a, b)

        return {'composite': composite, 'difference': difference}                
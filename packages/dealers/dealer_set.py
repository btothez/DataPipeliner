""" 
    Each customer "row" should have one of these classes dealer_set: dealer_set_obj
    The dealer_set's main object is a dict
    {delegate_dealer_id : [ customer_info_object_1, customer_info_object_2 ]}
"""

class DealerSet:
    def __init__(self, delegate_dealer_id, customer_info_obj):
        self.obj = {delegate_dealer_id : [ customer_info_obj ]}
        self.new_dealers = []

    def add_customer_info(self, delegate_dealer_id, customer_info_obj):
        if delegate_dealer_id in self.obj:

            if customer_info_obj.hasNewInfo(self.obj[delegate_dealer_id]):
                self.obj[delegate_dealer_id].append(customer_info_obj)

        else:
            self.obj[delegate_dealer_id] = [customer_info_obj]

    def incorporate_dealer_set(self, new_dealer_set):
        for dealer, obj in new_dealer_set.get_dealer_obj_tuples():
           self.add_customer_info(dealer, obj)

    def keys(self):
        return list(self.obj.keys())

    def get_customer_obj_list(self, dealer_id):
        return self.obj[dealer_id]

    def get_dealer_obj_tuples(self):
        tups = []
        for dealer in self.obj:
            for obj in self.obj[dealer]:
                tups.append((dealer, obj))

        return tups






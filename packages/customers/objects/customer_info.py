import re

""" 
    This class represents one customers information 
    (For a specific Dealer)
"""

class CustomerInfo:

    def __init__(self, customer_dict):
        self.customer_dict = self.addFeaturesToCustomerDict(customer_dict)

    def addFeaturesToCustomerDict(self, customer_dict):
        
        number_fields = ['home_phone', 'work_phone', 'cell_phone']
        email_fields = ['email_1', 'email_2', 'email_3']

        customer_dict['middle_initial'] = customer_dict['middle_name'][:1]
        customer_dict['numbers'] = sorted([re.sub("[^0-9]", "", customer_dict[field]) for field in number_fields if customer_dict[field]])
        customer_dict['emails'] = sorted([customer_dict[field] for field in email_fields if customer_dict[field]])
        
        return customer_dict

    def setInfo(self, customer_dict):
        self.customer_dict = self.addFeaturesToCustomerDict(customer_dict)

    def setCustomerInfoId(self, id):
        self.customer_dict['customer_info_id'] = id

    def setCustomerId(self, id):
        self.customer_dict['customer_id'] = id

    def setAddThis(self):
        self.customer_dict['add_this'] = True

    def getCustomerInfoId(self):
        return self.customer_dict['customer_info_id']

    def getCustomerId(self):
        return self.customer_dict['customer_id']

    def getDelegateDealerId(self):
        return self.customer_dict['delegate_dealer_id']

    def getCustomerDict(self):
        return self.customer_dict

    def getAddThis(self):
        if 'add_this' in self.customer_dict:
            return self.customer_dict['add_this']
        else:
            return False

    def hasNewInfo(self, customer_info_list):
        if len(customer_info_list) == 0:
            return True
            
        customer_dict = self.getCustomerDict()
        customer_dict_list = list(map(
            lambda ci: ci.getCustomerDict(), customer_info_list
        ))
        
        numbers_lists = [ci['numbers'] for ci in customer_dict_list]

        all_numbers = set().union(*numbers_lists)
        if not set(customer_dict['numbers']).issubset(set(all_numbers)):
            return True

        email_lists = [ci['emails'] for ci in customer_dict_list]
        all_emails = set().union(*email_lists)

        if not set(customer_dict['emails']).issubset(set(all_emails)):
            return True

        field_list = ['address_line_1', 'city', 'state', 'zip']

        for field in field_list:
            all_field_info = [ ci[field] 
                                for ci in customer_dict_list 
                                if ci[field] ]

            if customer_dict[field] and customer_dict[field] not in all_field_info:
                return True

        return False

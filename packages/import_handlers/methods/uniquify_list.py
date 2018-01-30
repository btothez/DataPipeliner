class UniquifyList:
    def __init__(self):
        self.customers_single_list = []
        self.uniq_key_list = [
            'full_name',
            'first_name', 
            'middle_name', 
            'last_name', 
            'address_line_1', 
            'email_1', 
            'city', 
            'home_phone', 
            'cell_phone',
            'delegate_dealer_id'
        ]

    def __call__(self, rows):    
        """ Get a uniq list of customers
            Fill up self.customersSingleList and
            self customersNonDictList """

        key_count = {}

        # Filter the rowlist
        for row in rows:
            unique_key = self.return_uniq_key(row)
            key_count[unique_key] = key_count.get(unique_key, 0) + 1
            if key_count[unique_key] == 1:
                self.customers_single_list.append(row)            

    def return_uniq_key(self, row):
        key_str = ''

        for key in self.uniq_key_list:
            row.setdefault(key, '')
            key_str += row[key]
        return key_str

class DictToTuple:
	def __init__(self, timestamp=0):
		self.timestamp = timestamp

	def defineFieldList(self, field_list):
		self.field_list = field_list

	def transform(self, row):
		newList = []		
		for element in self.field_list:	
			if element == 'import_time':
				row.setdefault(element, self.timestamp)
			else: 
				row.setdefault(element, None)
			
			newList.append(row[element])
			
		return tuple(newList)


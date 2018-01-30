import datetime

class Helpers:
    def __init__(self, table=None):
        self.table = table

    def chunkIt(self, long_list, chunk_size=19000):
        print(self.myxrange(0, len(long_list), chunk_size))
        chunks = [long_list[x:x + chunk_size] for x in self.myxrange(0, len(long_list), chunk_size)]
        return chunks    

    def myxrange(self, a, b, c):
        return filter(lambda x: x % c == 0, range(a,b))


    def dict_to_tuple(self, row, fields, timestamp):
        val_list = []

        for field in fields:
            if field == 'import_time':
                val_list.append(timestamp)
            elif field not in row:
                val_list.append('')
            else:
                val_list.append(row[field])

        return tuple(val_list)

    def dict_to_tuple_csv(self, row, fields, timestamp):
        val_list = []

        for field in fields:
            if field == 'import_time':
                val_list.append(str(timestamp))
            elif field not in row:
                val_list.append(None)

            elif type(row[field]) == datetime.date:
                val_list.append(row[field].strftime('%Y-%m-%d'))

            else: 
                val_list.append(row[field])
        return tuple(val_list)

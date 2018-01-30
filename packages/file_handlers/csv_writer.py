class CsvWriter:
    def __init__(self):
        pass

    def __call__(self, filename):
        with open(filename, 'a') as handle:
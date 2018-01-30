class PurifyName:
    
    def __call__(self, full_name):
        pieces = full_name.split(' ') 
        pieces = list(filter(
            lambda p: p.lower() != 'jr' and p.lower() != 'sr', pieces
        ))
        return ' '.join([pieces[0], pieces[len(pieces) - 1]])

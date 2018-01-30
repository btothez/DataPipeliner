import os
import sys

class AddColumns():
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def process(self, filename, newname):
        tmpname = "tmp_{}".format(filename)

        with open(filename) as handle:
            lines = handle.readlines()

        header = lines.pop(0)

        header = header.split("\t")
        header = self.replace_newline_last_col(header)

        header.append('Import Time')
        header.append('Dupe')

        header_str = "\t".join(header)

        new_lines = [ header_str ]

        for line in lines:
            line = line.split('\t')
            line = self.replace_newline_last_col(line)

            line.append(str(self.timestamp))
            line.append('0')
            line_str = '\t'.join(line)
            new_lines.append(line_str)

        with open(newname, 'w') as newhandle:
            newhandle.write("\n".join(new_lines))

    def replace_newline_last_col(self, line_pieces):
        last = line_pieces.pop().replace('\n', '')
        line_pieces.append(last)
        return line_pieces







"""
Example of usage - reformatting Agile PLM Mask Detail Report: joining splitted lines, columns
"""

import mutate_table as mt
import logging
import csv


def sort_column_value(s):
    tmp = s.split(",")
    tmp.sort()
    return ",".join(tmp)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    with mt.TableFromCSV("Privilege_Mask_Detail_Report.csv",
                         is_header=lambda row_number, row: len(row) > 0 and row[0] == 'Privilege Name') \
                            as source_table:

        mutated_table = source_table.mutate(mt.JoinColumns(9, 15))
        mutated_table = mutated_table.mutate(mt.JoinColumns(10, len(mutated_table.header), ';'))
        mutated_table = mutated_table.mutate(mt.JoinSplitLines())
        mutated_table = mutated_table.mutate(mt.MapColumn(6, sort_column_value))

        with open('output.csv', 'w') as out_handler:
            csv_file = csv.writer(out_handler)
            csv_file.writerow(mutated_table.header)

            for row in mutated_table.rows():
                csv_file.writerow(row)

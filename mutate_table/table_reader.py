import logging
import csv
from mutate_table.base_entities import Table

class TableFromLists(Table):
    """
    Table created from 2 lists - header, rows. Used for testing
    """

    def __init__(self, header, rows):
        super().__init__(header)
        self.rows = rows

    def rows(self):
        for row in self.rows:
            yield row


class TableFromCSV(Table):
    """
    Class used for reading CSV file
    """

    def __init__(self, file_name, is_header=None):
        super().__init__(None)
        self.file_name = file_name
        self.csv_file = None
        self.csv_reader = None
        self.line_number = 0

        self.log = logging.getLogger(self.__class__.__name__)

        # by default first row is a header row
        if is_header is None:
            self.is_header = lambda row_number, row: row_number == 1
        else:
            self.is_header = is_header

    def __find_header(self):
        """
        Finds header row and sets corresponding attribute
        """
        for row in self.csv_reader:
            self.line_number += 1

            if self.is_header(self.line_number, row):
                self.header = row
                self.log.debug("Header found at line " + str(self.line_number) + ": " + str(self.header))
                return

        self.log.error("Unable to find header in csv file " + self.file_name)

    def __enter__(self):
        self.log.debug("With statement enter")
        self.csv_file = open(self.file_name, "r", newline='')
        self.csv_reader = csv.reader(self.csv_file, delimiter=',', quotechar='"')
        self.__find_header()
        return self

    def rows(self):
        """
        Iterates over rest of csv file 

        :return: 
        """
        for row in self.csv_reader:
            yield row

    def __exit__(self, exc_type, exc_value, traceback):
        self.log.debug("With statement exit")
        self.csv_file.close()
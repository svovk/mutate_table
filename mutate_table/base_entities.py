import logging


class Table:
    """
    Class shows how table looks like - it has header attribute, rows method that yield rows, and also may apply mutation
    to itself
    """

    def __init__(self, header):
        self.header = header

    def rows(self):
        yield None

    def mutate(self, mutation):
        return MutatedTable(self, mutation)


class MutatedTable(Table):
    """
    Table with applied Mutation
    """

    def __init__(self, table, mutation):
        super().__init__(mutation.mutate_header(table.header))
        self.source_table = table
        self.mutation = mutation
        self.log = logging.getLogger(self.__class__.__name__)
        self.line_number = 0

    def rows(self):
        """
        This method applies mutation to each row of the source table. It yields a row as soon as Mutation object returns
        some value that is not None.
        """
        self.log.debug("Processing rows "+self.mutation.__class__.__name__)
        for row in self.source_table.rows():
            self.line_number += 1
            mutated_row = self.mutation.mutate_row(row)

            if mutated_row is not None:

                if len(mutated_row) != len(self.header):
                    self.log.warning(("Line {}: Number of values row parser returned({}) doesn't match to number of "
                                      "values in header row({})").format(self.line_number, len(mutated_row),
                                                                         len(self.header)))
                yield mutated_row

        mutated_row = self.mutation.end_of_rows()
        if mutated_row is not None:
            yield mutated_row


class Mutation:
    """
    Mutation that actually doesn't change anything
    """

    def mutate_header(self, header):
        """
        Called to mutate header

        :param header: 
        :return: 
        """
        return header

    def mutate_row(self, row):
        """
        Called for each row

        :param row: 
        :return: 
        """
        return row

    def end_of_rows(self):
        """
        Called when end of row is reached

        :return: 
        """
        return None
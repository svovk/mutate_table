from mutate_table.base_entities import Mutation


class JoinSplitLines(Mutation):
    """
    Mutation to read tables where several subsequent rows in the source table corresponds to one logical row. It joins
    physical rows into logical.
    """

    @staticmethod
    def is_next_logical_row(row):
        """
        Returns True if 1st attribute is filled

        :param row: list with a row value
        :return: logical value indicating whether current row is a new logical record  
        """
        if row[0] == '':
            return False
        else:
            return True

    @staticmethod
    def concatenate_cells(first, second, glue):
        if first is None:
            return second

        if second is None:
            return first

        return first + glue + second

    @staticmethod
    def concatenate_rows(first, second):
        if first is None:
            return list(second)

        if second is None:
            return list(first)

        max_len = max(len(first), len(second))

        result = [None] * max_len
        for i in range(0, max_len):
            result[i] = JoinSplitLines.concatenate_cells(
                first[i] if i < len(first) else None,
                second[i] if i < len(second) else None,
                '\n'
            )

        return result

    def __init__(self):
        super().__init__()
        self.accumulated = None

    def mutate_header(self, row):
        """
        This method is used to process header row

        :param row: header row
        :return: 
        """
        return row

    def mutate_row(self, row):
        """
        Invoked for each row after header row

        :param row: row 
        :return: either tuple with values to be or None 
        """
        if self.is_next_logical_row(row):
            ret = self.accumulated
            self.accumulated = row
            # TODO: Better way to trim trailing newlines - don't create them - store number of blank lines and insert them only if needed
            if ret is not None:
                return [cell.rstrip('\n') for cell in ret]
        else:
            self.accumulated = JoinSplitLines.concatenate_rows(self.accumulated, row)
            return None

    def end_of_rows(self):
        """
        Invoked when end of table rows is reached

        :return: 
        """
        # TODO: Better way to trim trailing newlines - don't create them - store number of blank lines and insert them only if needed
        return [cell.rstrip('\n') for cell in self.accumulated]


class JoinColumns(Mutation):
    """
    Mutation that joins cells of certain columns for each tow 
    """

    def __init__(self, from_column, to_column, glue=' '):
        super().__init__()
        self.from_column = from_column
        self.to_column = to_column
        self.glue = glue

    def mutate_header(self, header):
        """
        Called to mutate header

        :param header: 
        :return: 
        """
        result = header[:self.from_column]
        # TODO: refactor to use something better than rstrip
        result.append(self.glue.join(header[self.from_column:self.to_column]).rstrip(self.glue))
        result += header[self.to_column:]
        return result

    def mutate_row(self, row):
        """
        Called for each row

        :param row: 
        :return: 
        """
        result = row[:self.from_column]
        # TODO: refactor to use something better than rstrip
        result.append(self.glue.join(row[self.from_column:self.to_column]).rstrip(self.glue))
        result += row[self.to_column:]
        return result

    def end_of_rows(self):
        """
        Called when end of row is reached

        :return: 
        """
        return None

import unittest
import mutate_table as mt


class JoinColumnsTestCase(unittest.TestCase):

    def setUp(self):
        self.mutator = mt.JoinColumns(3, 5, '')

    def test_header(self):
        mutated_header = self.mutator.mutate_header(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_header, ['a', 'b', 'c', 'de', 'f'])

    def test_row(self):
        mutated_row = self.mutator.mutate_row(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_row, ['a', 'b', 'c', 'de', 'f'])


class JoinSplitterLinesTestCase(unittest.TestCase):

    def setUp(self):
        self.mutator = mt.JoinSplitLines()

    def test_header(self):
        mutated_header = self.mutator.mutate_header(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_header, ['a', 'b', 'c', 'd', 'e', 'f'])

    def test_row(self):
        # feed 1st row - nothing expected to be returned
        mutated_row = self.mutator.mutate_row(['1', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_row, None)

        # feed 2nd row - 1st row should be returned
        mutated_row = self.mutator.mutate_row(['2', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_row, ['1', 'b', 'c', 'd', 'e', 'f'])

        # feed 3rd row - nothing expected to be returned as it is a splitter line
        mutated_row = self.mutator.mutate_row(['', 'b', 'c', 'd', 'e', 'f'])
        self.assertEquals(mutated_row, None)

        # feed 4th row (which is 3rd logical row) - concatenated 2nd row should be returned
        mutated_row = self.mutator.mutate_row(['3', 'b', 'c', 'd', 'e', 'f'])
        self.assertEquals(mutated_row, ['2', 'b\nb', 'c\nc', 'd\nd', 'e\ne', 'f\nf'])

        # end of rows reached
        mutated_row = self.mutator.end_of_rows()
        self.assertEqual(mutated_row, ['3', 'b', 'c', 'd', 'e', 'f'])


class MapColumnTestCase(unittest.TestCase):

    def setUp(self):
        self.mutator = mt.MapColumn(2, lambda x: '!' if x == 'Z' else x)

    def test_header(self):
        mutated_header = self.mutator.mutate_header(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_header, ['a', 'b', 'c', 'd', 'e', 'f'])

    def test_row(self):
        mutated_row = self.mutator.mutate_row(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(mutated_row, ['a', 'b', 'c', 'd', 'e', 'f'])

        mutated_row = self.mutator.mutate_row(['a', 'b', 'Z', 'd', 'e', 'f'])
        self.assertEqual(mutated_row, ['a', 'b', '!', 'd', 'e', 'f'])


if __name__ == '__main__':
    unittest.main()

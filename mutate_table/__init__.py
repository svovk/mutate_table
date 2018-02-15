"""
Mutate Table - tiny set of classes that might be used to simplify table reformatting
"""

from mutate_table.base_entities import Table, MutatedTable
from mutate_table.table_reader import TableFromCSV
from mutate_table.mutations import JoinColumns, JoinSplitLines, MapColumn

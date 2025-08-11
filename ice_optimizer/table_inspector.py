# # responsible for generating table metadata


class TableInspector:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns

import re
from typing import List


# ideia - várias operações
# valores vazios
# alerta para csvs vazios
# alerta para operações não suportadas
# alerta para linhas demasiado preenchidas
# alerta para valores não numéricos no csv


def process_header(header_line: str) -> (List[str], List[str]):
    """Retrieves information about the fields declared in the header

    Args:
        header_line (str): First line of the csv file

    Raises:
        NameError: Unsupported operation is found in the header

    Returns:
        [list(str),list(str)]: 
        column_names (list(str)): List of the names of each column,
        column_operations (list(str)): List where each index corresponds to the type of operation to be applied to the corresponding column by index
    """

    column_names = []
    column_operations = []
    supported_folds = ["sum", "avg", "max", "min"]
    captures = re.findall(r'([^*;]+)(\*)?([^;]+)?', header_line);

    for capture in captures:
        num_clauses = len(list(filter(None, capture)))
        column_names.append(capture[0])
        if num_clauses == 1:
            column_operations.append("none")
        elif num_clauses == 2:
            column_operations.append("group")
        else:  # 3
            operation = capture[2]
            if not (operation.lower() in supported_folds):
                raise NameError("Unsupported Operation in header: " + operation)
            else:
                column_operations.append(operation.lower())

    return column_names, column_operations


def process_body(csv_lines: List[str],
                 column_names: List[str],
                 column_operations: List[str]):
    """Processes each line of the csv

    Args:
        csv_lines (list[str]): Each line of the csv
        column_names (list[str]): List of the names of each column
        column_operations (list[str]): List where each index corresponds to the type of operation to be applied to the corresponding column by index

    Raises:
        AttributeError: If there's a row with a different number of columns than defined by the header
        AttributeError: If a row contains an empty parenthesis on a group column
        ValueError: If a row contains non-numeric values on a group column
    """
    for i, line in enumerate(csv_lines):
        # capture a field delimited by either ';' 
        # or end of line OR capture an empty field
        fields = re.findall(r'([^;]+)(?:;|$)|;', line)
        if len(fields) != len(column_names):
            raise AttributeError(
                "Row " + str(i + 2) + " does not have the same number of columns as determined by the header")

        for j, field in enumerate(fields):
            print(column_names[j] + ": " + field)
            if column_operations[j] != "none":
                values = re.match(r'\(([^)]+)\)', field)  # extract the values inside the parenthesis
                if not values:
                    raise AttributeError("Row " + str(i + 2) + " presents empty parenthesis on a group column")
                values = list(re.findall(r'([^,]+)(?:,|$)',  # find values separated by commas
                                         values.group(1)))  # group(1) since we want what's inside
                                                            # the parenthesis, not the full match

                try:
                    numeric_values = [float(value) for value in values]
                    print("Numeric Values", numeric_values)
                except ValueError:
                    raise ValueError("Non numeric element in row " + str(j));
        print("-----")


file = open("data/data.csv")

lines = file.read().splitlines()  # splitlines to remove \n

file.close()

if len(lines) < 2:
    raise Exception("Insufficient lines in csv")

csv_column_names, csv_column_operations = process_header(lines[0])

process_body(lines[1:], csv_column_names, csv_column_operations)
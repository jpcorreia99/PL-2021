import re
import time

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
        [List(str),List(str)]: 
        column_names (List(str)): List of the names of each column,
        column_operations (List(str)): List where each index corresponds to the type of operation to be applied to the corresponding column by index
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
            column_operations.append(["group"])
        else:  # 3
            operations = [operation.lower() for operation in capture[2].split(",")]
            if any([operation not in supported_folds for operation in operations]):
                raise NameError("Unsupported Operation in header")
            else:
                column_operations.append(operations)

    return column_names, column_operations

def process_operations(column_name: str,
                      values: List[str],
                      operations: List[str],
                      last_column: bool) -> List[str]:

    """ Converts line portion corresponding to an operations column to it's json conterpart

    Args:
        column_name: Name of the column being processed,
        values: list of values present in said column,
        operations: list of operations to be applied to the values in the values list, matched by index
        last_column: flag indicating if it's the last column of the line being processed, for comma purposes

    Raises:
        ValueError:  If there's non-numeric values on a list of values

    Returns:
        List[str]: Json portion relative to the operations in the given collumn
    """
    operation_results = []
    try:
        numeric_values = [float(value) for value in values]
        for i, operation in enumerate(operations):
            if operation == "group":
                operation_result = f'\t\t"{column_name}": {numeric_values}'
            if operation == "avg":
                operation_result = f'\t\t"{column_name}_avg": {sum(numeric_values)/len(numeric_values)}'
            elif operation == "sum":
                operation_result = f'\t\t"{column_name}_sum": {sum(numeric_values)}'
            elif operation == "min":
                operation_result = f'\t\t"{column_name}_min": {min(numeric_values)}'
            elif operation == "max":
                operation_result = f'\t\t"{column_name}_max": {max(numeric_values)}'
                
            operation_results.append(operation_result + ("" if last_column and i==len(operations)-1 else ","))

        return operation_results
    except ValueError:
        raise ValueError("Non numeric element in row " + str(j)+" in a column that demands such");

def convert_to_json(csv_lines: List[str],
                 column_names: List[str],
                 column_operations: List[str]):
    """Processes each line of the body of the csv and converts it to a string in json format

    Args:
        csv_lines (List[str]): Each line of the csv
        column_names (List[str]): List of the names of each column
        column_operations (List[str]): List where each index corresponds to the type of operation to be applied to the corresponding column by index

    Raises:
        AttributeError: If there's a row with a different number of columns than defined by the header
        AttributeError: If a row contains an empty parenthesis on a group column
        ValueError: If a row contains non-numeric values on a group column
    """
    string_list = []

    string_list.append("[")
    for i, line in enumerate(csv_lines):
        string_list.append("\t{")
        # capture a field delimited by either ';' 
        # or end of line OR capture an empty field
        fields = re.findall(r'([^;]+)(?:;|$)|;', line)
        if len(fields) != len(column_names):
            raise AttributeError(
                "Row " + str(i + 2) + " does not have the same number of columns as determined by the header")

        for j, field in enumerate(fields):
            if column_operations[j] == "none":
                string_list.append(f'\t\t"{column_names[j]}": "{field}",')
            else:
                values = re.match(r'\(([^)]+)\)', field)  # extract the values inside the parenthesis
                if not values:
                    raise AttributeError("Row " + str(i + 2) + " presents empty parenthesis on a group column")
               
                values = list(re.findall(r'([^,]+)(?:,|$)',  # find values separated by commas
                                         values.group(1)))  # group(1) since we want what's inside
                                                     # the parenthesis, not the full match
                string_list = string_list + process_operations(column_names[j], values, column_operations[j],
                                                              j == len(fields)-1) # boolean indicating it's the last column of the line
        if(i == len(csv_lines)-1):
            string_list.append("\t}") # the last object doest not have a comma
        else:
            string_list.append("\t},")
            
    string_list.append("]")
    return '\n'.join(string_list)

# Reading the file
file = open("data/data.csv")
lines = file.read().splitlines()  # splitlines to remove \n
file.close()

if len(lines) < 2:
    raise Exception("Insufficient lines in csv")

# Processing the file
start = time.time()
csv_column_names, csv_column_operations = process_header(lines[0])
json_txt = convert_to_json(lines[1:], csv_column_names, csv_column_operations)
end = time.time()

# Writing to json
output_file = open("data/data.json", "w")
output_file.write(json_txt)
output_file.close()

print(f"Time spent processing the csv: {end - start}s")
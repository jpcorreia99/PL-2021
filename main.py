import re
import time
from typing import List
import sys

# ideia - várias operações
# valores vazios
# alerta para csvs vazios
# alerta para operações não suportadas
# alerta para linhas demasiado preenchidas
# alerta para valores não numéricos no csv
# operador group permite manter os valores
# operador de cast para valor numérico
# deteção automática do separador
# ler ficheiro de input e output

#TODO verificar o findall da linha 130, é mesmo necessário findall?
# A fazer ler argumentos
# detetar separador


def process_header(header_line: str) -> (List[str], List[str]):
    """Retrieves information about the fields declared in the header

    Args:
        header_line (str): First line of the csv file

    Raises:
        NameError: Unsupported operation is found in the header

    Returns:
        str: field, delimiter 
        str: character separating group operations, 
        [List(str),List(str)]: 
        column_names (List(str)): List of the names of each column,
        column_operations (List(str)): List where each index corresponds to the type of
        operation to be applied to the corresponding column by index
    """

    column_names = []
    column_operations = []
    supported_group_operations = ["group", "sum", "avg", "max", "min"]
    field_delimiter = re.match(r'(?:[^+*;,]+)(?:\*|\+)?(?:[^;,]+)?(;|,|$)',header_line).group(1)

    if field_delimiter == ';':
        operations_separator = ","
        captures = re.findall(r'([^+*;]+)(\*|\+)?([^;]+)?', header_line);
    else:
        field_delimiter = "," # needs to be set as ',' since $ will break the program when processing de body 
        operations_separator = ";"
        captures = re.findall(r'([^+*,]+)(\*|\+)?([^,]+)?', header_line);

    for capture in captures:
        num_clauses = len(list(filter(None, capture)))
        column_names.append(capture[0])
        if num_clauses == 1:
            column_operations.append("none")
        elif num_clauses == 2:
            if capture[1] == "*":
                column_operations.append(["group"])
            elif capture[1] == "+":
                column_operations.append("cast")
        else:  # 3
            operations = [operation.lower() for operation in capture[2].split(operations_separator)]
            if any([operation not in supported_group_operations for operation in operations]):
                raise NameError("Unsupported Operation in header")
            else:
                column_operations.append(operations)

    return field_delimiter, operations_separator,column_names, column_operations

def process_operations(column_name: str,
                      values: List[str],
                      operations: List[str],
                      row_number: int,
                      last_column: bool) -> List[str]:

    """ Converts line portion corresponding to an operations column to it's json conterpart

    Args:
        column_name: Name of the column being processed,
        values: list of values present in said column,
        operations: list of operations to be applied to the values in the values list, matched by index,
        row_number: useful for throwing informative exceptions
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
                
            operation_results.append(operation_result +
             ("" if last_column and i==len(operations)-1 else ",")) #also checks if it's the last operation of the given column

        return operation_results
    except ValueError:
        raise ValueError(f"Non numeric element in row {str(row_number)} in a column that demands such");


def convert_to_json(csv_lines: List[str],
                 field_delimiter: str,
                 operations_separator: str,
                 column_names: List[str],
                 column_operations: List[str]) -> str:
    """Processes each line of the body of the csv and converts it to a string in json format

    Args:
        csv_lines (List[str]): Each line of the csv
        column_names (List[str]): List of the names of each column
        spcolumn_operations (List[str]): List where each index corresponds to the type of operation
        to be applied to the corresponding column by index

    Raises:
        AttributeError: If there's a row with a different number of columns than defined by the header
        AttributeError: If a row contains an empty or missing parenthesis on a group column
        ValueError: If a row contains non-numeric values on a group column
        ValueError: If a row with a cast operation contains non-numeric values

    Returns:
        str: String containing the complete JSON file
    """
    string_list = []

    string_list.append("[")
    for i, line in enumerate(csv_lines):
        string_list.append("\t{")
        # capture a field delimited by either ';' 
        # or end of line OR capture an empty field
        # fields = re.findall(r'([^;]+)(?:;|$)|;', line)
        fields = line.split(field_delimiter)

        if len(fields) != len(column_names):
            raise AttributeError(
                f"Row {str(i + 2)} does not have the same number of columns as determined by the header")

        for j, field in enumerate(fields):
            if field: # skips empty fields
                if column_operations[j] == "none":
                    string_list.append(f'\t\t"{column_names[j]}": "{field}"' +
                        ("," if (len(list(filter(None,fields[j:]))) > 1) else "")) # condition checks if it's not the last non-empty field   
                elif column_operations[j]== "cast":
                    try:
                        numeric_value = float(field)
                        string_list.append(f'\t\t"{column_names[j]}": {numeric_value}' +
                        ("," if (len(list(filter(None,fields[j:]))) > 1) else "")) # condition checks if it's not the last non-empty field   
                    except ValueError:
                        raise ValueError(f"Row {str(i + 2)}: {field} can't be casted to a numeric value")
                else:
                    values = re.match(r'\(([^)]+)\)', field)  # extract the values inside the parenthesis, since it's a list column
                    if not values:
                        raise AttributeError(f"Row {str(i + 2)} presents empty parenthesis or no parenthesis on a group column")

                    # find values separated by the operations_separator, group(1) since we want what's inside the parenthesis, not the full match
                    #values = list(re.findall(fr'([^{operations_separator}]+)(?:{operations_separator}|$)', values.group(1))) 
                    values = values.group(1).split(operations_separator)

                    if len(values) > 0:
                        string_list = string_list + process_operations(column_names[j], values, column_operations[j],i + 2,
                                                                    # boolean indicating it's the last column of the line
                                                                    len(list(filter(None,fields[j:]))) == 1)
        if(i == len(csv_lines)-1):
            string_list.append("\t}") # the last object doest not have a comma
        else:
            string_list.append("\t},")
            
    string_list.append("]")
    return '\n'.join(string_list)



if len(sys.argv) == 1:
    input_file_path = "input/data.csv"
    output_file_path = f"output/data.json"
elif len(sys.argv) == 2: 
    input_file_path = f"input/{sys.argv[1]}"
    output_file_path = f"output/data.json"
elif len(sys.argv) == 3:
    input_file_path = f"input/{sys.argv[1]}"
    output_file_path = f"output/{sys.argv[2]}"
else:
    raise ValueError("Wrong number of arguments.\nUsage: python main.py [input_file_name] [output_file_name]")


# Reading the file
file = open(input_file_path)
lines = file.read().splitlines()  # splitlines to remove \n
file.close()

if len(lines) < 2:
    raise Exception("Insufficient lines in csv")

# Processing the file
start = time.time()
field_delimiter, operations_separator, csv_column_names, csv_column_operations = process_header(lines[0])
json_txt = convert_to_json(lines[1:],field_delimiter, operations_separator,csv_column_names, csv_column_operations)
end = time.time()

# Writing to json
output_file = open(output_file_path, "w")
output_file.write(json_txt)
output_file.close()

print(f"Time spent processing the csv: {end - start}s")

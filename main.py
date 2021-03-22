import re

#ideia - várias operações

def process_header(header_line):
    column_names = []
    column_operations = []
    supported_folds = ["sum","avg","max","min"]
    captures = re.findall(r'([^*;]+)(\*)?([^;]+)?',header_line);

    for capture in captures:
        num_clauses = len(list(filter(None,capture)))
        column_names.append(capture[0])
        if num_clauses == 1:
            column_operations.append("none")
        elif num_clauses == 2:
            column_operations.append("gather")
        else: # 3
            operation = capture[2]
            if not (operation.lower() in supported_folds):
                raise ValueError("Unsuported Operation in header: "+operation)
            else:
                column_operations.append(operation.lower())

    return column_names, column_operations


file = open("data/data.csv")

lines = file.read().splitlines() # splitlines to remove \n

file.close()

if len(lines) < 2:
    raise Exception("Unsuficient lines in csv") 

column_names, column_operations = process_header(lines[0])
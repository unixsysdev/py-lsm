import pandas as pd
import time
import re
import numpy as np

import pandas as pd
import numpy as np
import re
import time
import random


class SQLLayer:
    def __init__(self, df, table_name='table'):
        self.df = df
        self.table_name = table_name

    def execute(self, query):
        query_type = query.split()[0].upper()
        
        if query_type == 'SELECT':
            return self._execute_select(query)
        elif query_type == 'DELETE':
            return self._execute_delete(query)
        elif query_type == 'INSERT':
            return self._execute_insert(query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def _execute_select(self, query):
        # Updated regex to correctly separate all clauses
        pattern = r'SELECT (.*?) FROM (\w+)(?: WHERE (.*?))?(?: GROUP BY (.*?))?(?: ORDER BY (.*?))?(?: LIMIT (\d+))?$'
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise ValueError("Invalid SELECT query format")

        select_clause, table_name, where_clause, group_by_clause, order_by_clause, limit_clause = match.groups()

        # Check if the table name matches
        if table_name.lower() != self.table_name.lower():
            raise ValueError(f"Invalid table name. Expected '{self.table_name}', got '{table_name}'")

        result = self.df

        # Process WHERE clause using vectorized operations
        if where_clause:
            result = result[self._vectorized_where(where_clause)]

        # Process GROUP BY clause
        if group_by_clause:
            group_cols = [col.strip() for col in group_by_clause.split(',')]
            result = self._vectorized_group_by(result, group_cols, select_clause)
        
        # Process SELECT clause
        select_cols = [col.strip() for col in select_clause.split(',')]
        result = self._vectorized_select(result, select_cols)

        # Process ORDER BY clause
        if order_by_clause:
            order_cols = [col.strip() for col in order_by_clause.split(',')]
            ascending = [not col.endswith(' DESC') for col in order_cols]
            order_cols = [col.replace(' ASC', '').replace(' DESC', '') for col in order_cols]
            result = result.sort_values(order_cols, ascending=ascending)

        # Process LIMIT clause
        if limit_clause:
            result = result.head(int(limit_clause))

        return result

    def _vectorized_where(self, condition):
        def parse_condition(cond):
            parts = re.split(r'(=|!=|>|<|>=|<=|AND|OR)', cond)
            parts = [part.strip() for part in parts if part.strip()]
            return parts

        def apply_condition(left, op, right):
            if op == '=':
                return self.df[left] == right
            elif op == '!=':
                return self.df[left] != right
            elif op == '>':
                return self.df[left] > float(right)
            elif op == '<':
                return self.df[left] < float(right)
            elif op == '>=':
                return self.df[left] >= float(right)
            elif op == '<=':
                return self.df[left] <= float(right)

        parts = parse_condition(condition)
        result = None
        i = 0
        while i < len(parts):
            if parts[i] in self.df.columns:
                left, op, right = parts[i:i+3]
                if right.startswith("'") and right.endswith("'"):
                    right = right[1:-1]  # Remove quotes for string values
                condition_result = apply_condition(left, op, right)
                if result is None:
                    result = condition_result
                else:
                    if parts[i-1] == 'AND':
                        result = result & condition_result
                    elif parts[i-1] == 'OR':
                        result = result | condition_result
                i += 3
            else:
                i += 1

        return result

    def _vectorized_group_by(self, df, group_cols, select_clause):
        agg_funcs = {'SUM': 'sum', 'AVG': 'mean', 'MIN': 'min', 'MAX': 'max', 'COUNT': 'count'}
        agg_dict = {}

        for col in select_clause.split(','):
            col = col.strip()
            for func in agg_funcs:
                if col.upper().startswith(func):
                    col_name = col[len(func)+1:-1]  # Remove function name and parentheses
                    agg_dict[col_name] = agg_funcs[func]
                    break
            else:
                if col not in group_cols:
                    agg_dict[col] = 'first'

        return df.groupby(group_cols).agg(agg_dict).reset_index()

    def _vectorized_select(self, df, select_cols):
        agg_funcs = {'SUM': np.sum, 'AVG': np.mean, 'MIN': np.min, 'MAX': np.max, 'COUNT': np.count_nonzero}
        
        result = pd.DataFrame()
        for col in select_cols:
            for func in agg_funcs:
                if col.upper().startswith(func):
                    col_name = col[len(func)+1:-1]  # Remove function name and parentheses
                    result[col] = agg_funcs[func](df[col_name])
                    break
            else:
                result[col] = df[col]
        
        return result

    def _execute_delete(self, query):
        match = re.match(r'DELETE FROM (.*?)(?: WHERE (.*))?$', query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid DELETE query format")

        from_clause, where_clause = match.groups()

        if from_clause.strip().lower() != self.table_name.lower():
            raise ValueError(f"Only '{self.table_name}' is supported in the FROM clause")

        if where_clause:
            mask = self._vectorized_where(where_clause)
            rows_deleted = (~mask).sum()
            self.df = self.df[mask]
        else:
            rows_deleted = len(self.df)
            self.df = self.df.iloc[0:0]

        return f"Deleted {rows_deleted} rows from {self.table_name}"

    def _execute_insert(self, query):
        match = re.match(r'INSERT INTO (.*?) \((.*?)\) VALUES \((.*?)\)$', query, re.IGNORECASE)
        
        if not match:
            raise ValueError("Invalid INSERT query format")

        into_clause, columns, values = match.groups()

        if into_clause.strip().lower() != self.table_name.lower():
            raise ValueError(f"Only '{self.table_name}' is supported in the INTO clause")

        columns = [col.strip() for col in columns.split(',')]
        values = [val.strip() for val in values.split(',')]

        if len(columns) != len(values):
            raise ValueError("Number of columns doesn't match number of values")

        new_row = pd.DataFrame([dict(zip(columns, values))])
        self.df = pd.concat([self.df, new_row], ignore_index=True)

        return f"Inserted 1 row into {self.table_name}"

# Performance testing function
def run_performance_test(sql_layer, num_tests=50):
    def time_execution(query):
        start_time = time.time()
        result = sql_layer.execute(query)
        execution_time = time.time() - start_time
        return result, execution_time

    tests = [
        ("Simple SELECT", f"SELECT A, B, C FROM {sql_layer.table_name} LIMIT 1000"),
        ("SELECT with WHERE", f"SELECT A, B, C FROM {sql_layer.table_name} WHERE A > 500000 AND B = 'x'"),
        ("SELECT with GROUP BY", f"SELECT B, AVG(C) FROM {sql_layer.table_name} GROUP BY B"),
        ("Complex SELECT", f"SELECT B, AVG(C), MAX(A) FROM {sql_layer.table_name} WHERE A > 250000 GROUP BY B ORDER BY AVG(C) DESC"),
        ("DELETE", f"DELETE FROM {sql_layer.table_name} WHERE A < 100"),
        ("INSERT", f"INSERT INTO {sql_layer.table_name} (A, B, C) VALUES (2000001, 'w', 150)")
    ]

    print("\nPerformance Test Results:")
    print("="*50)

    for test_name, query in tests:
        total_time = 0
        for i in range(num_tests):
            if test_name == "DELETE" or test_name == "INSERT":
                # For DELETE and INSERT, we need to reset the dataframe each time
                sql_layer.df = pd.DataFrame({
                    'A': range(1, 1000001),
                    'B': np.random.choice(['x', 'y', 'z'], 1000000),
                    'C': np.random.randint(1, 101, 1000000)
                })
            
            _, execution_time = time_execution(query)
            total_time += execution_time
        
        avg_time = total_time / num_tests
        print(f"{test_name:20}: {avg_time:.4f} seconds (averaged over {num_tests} runs)")

    return total_time

# Create a large DataFrame for initial testing
df = pd.DataFrame({
    'A': range(1, 1000001),
    'B': np.random.choice(['x', 'y', 'z'], 1000000),
    'C': np.random.randint(1, 101, 1000000)
})

sql_layer = SQLLayer(df, table_name='my_table')  # Use a custom table name

# Run the performance test
run_performance_test(sql_layer)

# Scalability Test
print("\nScalability Test:")
print("="*50)
for size in [50000000]:
    df = pd.DataFrame({
        'A': range(1, size+1),
        'B': np.random.choice(['x', 'y', 'z'], size),
        'C': np.random.randint(1, 101, size)
    })
    sql_layer = SQLLayer(df, table_name='scalability_test')
    total_time = run_performance_test(sql_layer, num_tests=1)
    print(f"\nDataset size: {size}")
    print(f"Total execution time: {total_time:.4f} seconds")

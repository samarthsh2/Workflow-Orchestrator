file_and_dependecies = [
    {
        "file_name": "python_test_file",
        "format": "py",
        "dependent_on": ["sql_test_file"],
        "compute_name": "test compute cluster"
    },
    {
        "file_name": "python_test_file_2",
        "format": "py",
        "dependent_on": ["sql_test_file"],
        "compute_name": "test compute cluster 2"
    },
    {
        "file_name": "sql_test_file",
        "format": "sql",
        "dependent_on": ["sql_test_file_pre"],
        "warehouse_name": "test_sql_warehouse_samarth_2"
    },
    {
        "file_name": "sql_test_file_pre",
        "format": "sql",
        "dependent_on": [],
        "warehouse_name": "test_sql_warehouse_samarth"
    },
    {
        "file_name": "final_result",
        "format": "py",
        "dependent_on": ["python_test_file", "python_test_file_2"],
        "compute_name": "test compute cluster"
    },
]
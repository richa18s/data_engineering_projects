class DataQualityChecks:
    check_nulls = {'check_name': 'Null userids', 
                        'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 
                        'expected_result': 0}
    
    list_of_checks = [check_nulls]
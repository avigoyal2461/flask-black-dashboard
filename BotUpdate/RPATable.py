from BotUpdate.dbconnection import connection
from datetime import datetime, timezone

class RPATable(connection):
    def __init__(self, schema, table, config_path="DEV"):
        """
        Extends dbconnection, each method calls on dbconnection to either execute or read a query
        This class is specific to RPA tables, tables are printed if not initalized with a table in mind
        init looks like : a = RPATable('Process')
        This class will tell dbconnection that we are loading using the RPA user
        """
        rpa_tables = ['Process', 'Process_Detail', 'Process_Run', 'Queue_Item', 'Job_Posting', 'Database_DDL_Audit']

        if table in rpa_tables:
            super().__init__(config_path)
            self.schema = schema
            self.table = table
        else:
            return f"Invalid Table / No Table Argument Given, valid tables are: {rpa_tables}"

    def Read(self, query):
        return super().Read(query)

    def Execute(self, query):
        return super().Execute(query)

    def create_timestamp(self):
        """
        Helper method to create a timestamp if not given
        """
        #time = datetime.today()
        now = datetime.now(timezone.utc)
        time = now.strftime('%Y-%m-%d %H:%M:%S')
        #nyc = pytz.timezone("America/New_York") 
        #time = datetime.now(nyc).strftime('%Y-%m-%d %H:%M:%S')
        return time

    def build_update_string(self, value_dictionary):
        """
        Helper method
        Builds the update string from a dictionary value
        """
        update_string = ""
        for index, key in enumerate(value_dictionary):
            if index == 0:
                update_string += f"f.{key} = '{value_dictionary[key]}'"
            else:
                update_string += f", f.{key} = '{value_dictionary[key]}'"

        return update_string

    def Select_All(self):
        """
        Selects all tables from initialized DB Table
        """
        query = f"""
        SELECT *
        FROM [{self.schema}].[{self.table}]
        """

        data = super().Read(query)
        return data

    def Select(self, where_clause, column_names):
        """
        Selects specific rows based on given list - SEARCHES BY PROCESS ID
        Takes column names as a list entry : ['Process_Name', 'Process_Description'] - 
        if only one value : ['Process_Name']
        """
        select_string = ""
        for index, value in enumerate(column_names):
            if index == 0:
                select_string += f"{value}"
            else:
                select_string += f", {value}"

        query = f"""
        SELECT {select_string}
        FROM [{self.schema}].[{self.table}]
        {where_clause}
        """

        data = super().Read(query)
        return data 

    def Select_Extra_Identifier(self, process_id, column_names, where_clause):
        """
        Selects specific rows based on given list - SEARCHES BY PROCESS ID
        Takes column names as a list entry : ['Process_Name', 'Process_Description'] - 
        if only one value : ['Process_Name']
        """
        select_string = ""
        for index, value in enumerate(column_names):
            if index == 0:
                select_string += f"{value}"
            else:
                select_string += f", {value}"

        query = f"""
        SELECT {select_string}
        FROM [{self.schema}].[{self.table}]
        WHERE Process_Id = {process_id} and {where_clause}
        """

        data = super().Read(query)
        return data 

    def Insert(self, value_dictionary):
        """
        Takes a dictionary of values - columnName : value
        and inserts into desired table , ex -
        #Process table
        value_dictionary = {
            'Process_Name' : 'Test',
            'Process_Description': 'Test Insert',
            'Is_Continuous': 1
        }
        """
        insert_string = ""
        value_string = ""

        for index, key in enumerate(value_dictionary):
            if index == 0:
                insert_string += f"{key}"
            else:
                insert_string += f", {key}"

        for index, key in enumerate(value_dictionary):
            if index == 0:
                value_string += f"'{value_dictionary[key]}'"
            else:
                value_string += f", '{value_dictionary[key]}'"

        query = f"""
        INSERT INTO [{self.schema}].[{self.table}] ({insert_string})
        VALUES ({value_string})
        """

        #data will be the primary key on newly inserted value
        data = super().Execute(query)
        return data
    
    def Delete(self, process_id):
        """
        Deletes entry from table
        """

        query = f"""
        DELETE FROM [{self.schema}].[{self.table}]
        WHERE Process_Id = {process_id}
        """
        data = super().Execute(query)
        return data

    def get_process_id(self, process_name):
        """
        Takes Process Name Input and returns the ID
        NEEDS TO BE IN THE PROCESS TABLE
        """

        query = f""" 
        SELECT Process_Id
        FROM [{self.schema}].[Process]
        WHERE Process_Name = '{process_name}'
        """

        data = super().Read(query)
        if data.empty:
            return "Entry not found in RPA Process Table"
        else:
            data = data['Process_Id'][0]
        return data
    
    def get_process_run_id(self, process_id):
        """
        Takes process ID and pulls process run id from the 
        process run table
        """
        query = f""" 
        SELECT Process_Run_Id
        FROM [{self.schema}].[Process_Run]
        WHERE Process_Id = '{process_id}' AND Process_Run_Status = 'I'
        """
        data = super().Read(query)
        if data.empty:
            return "Process Run ID not found in RPA Process Table"
        else:
            data = data['Process_Run_Id'][0]
        return data
    
    def Update_extra_identifier(self, whereclause, value_dictionary):
        """
        Takes a dictionary of values - columnName : value
        and updates the desired table, this will take a where clause as a variable, used to add "AND" to updates
        THIS WILL ONLY DO ONE ROW AT A TIME 
        ex -
        #Process table
        value_dictionary = {
            'Process_Name' : 'Test',
            'Process_Description': 'Test Insert',
            'Is_Continuous': 1
            }
        """
        update_string = self.build_update_string(value_dictionary)

        query = f"""
        UPDATE f
        SET {update_string}
        FROM [{self.schema}].[{self.table}] f
        {whereclause}
        """

        data = super().Execute(query)
        return data

    def Update(self, process_id, value_dictionary):
        """
        Takes a dictionary of values - columnName : value
        and updates the desired table,
        THIS WILL ONLY DO ONE COLUMN AT A TIME 
        ex -
        #Process table
        value_dictionary = {
            'Process_Name' : 'Test',
            'Process_Description': 'Test Insert',
            'Is_Continuous': 1
            }
        """

        update_string = self.build_update_string(value_dictionary)
        process_id = str(process_id)
        
        query = f"""
        UPDATE f
        SET {update_string}
        FROM [{self.schema}].[{self.table}] f
        WHERE Process_Id = '{process_id}'
        """

        data = super().Execute(query)
        return data
    
    def Update_Process_run(self, process_run_id, value_dictionary):
        """
        Takes a dictionary of values - columnName : value
        and updates the desired table,
        THIS WILL ONLY DO ONE COLUMN AT A TIME 
        ex -
        #Process table
        value_dictionary = {
            'Process_Name' : 'Test',
            'Process_Description': 'Test Insert',
            'Is_Continuous': 1
            }
        """
        update_string = self.build_update_string(value_dictionary)

        process_run_id = str(process_run_id)
        
        query = f"""
        UPDATE f
        SET {update_string}
        FROM [{self.schema}].[{self.table}] f
        WHERE Process_Run_Id = '{process_run_id}'
        """

        data = super().Execute(query)
        return data

a = RPATable("RPA", "Process")#.Select(["Process_Id"])
print(a.create_timestamp())

"""
Code Graveyard
"""
# query = """
#         INSERT INTO [""" + self.schema + """].[""" + self.table + """] (""" + insert_string + """)
#         VALUES (""" + value_string + """)
#         """
    # def Update_Time(self, time=None, process_name=None):
    #     """
    #     Updates Last updated time in the process table
    #     """
    #     # cursor = self.connection.cursor()
    #     if not time:
    #         time = self.create_timestamp()

    #     if not process_name:
    #         return "Please Add a process_name to continue (to view table try : Select_All)"

    #     query = """
    #     UPDATE f 
    #     SET f.Last_Updated_Timestamp = '""" + time + """'
    #     from """ + self.schema + """.""" + self.table +""" f
    #     where f.Process_Name = '""" + process_name + """'
    #     """
    #     data = super().Execute(query)
# query = """
        # SELECT """ + select_string + """
        # FROM [""" + self.schema + """].[""" + self.table + """]
        # """
    #     return data

from BotUpdate.RPATable import RPATable
from datetime import datetime 
import time

class RPA_ProcessDetail_Table(RPATable):
    def __init__(self):
        print("Initializing RPA Process Detail Table")
        self.table = "Process_Detail"
        self.schema = "rpa"
        super().__init__(self.schema, self.table)

    def get_process_id(self, process_name):
        return super().get_process_id(process_name)
    
    def get_process_run_id(self, process_id):
        return super().get_process_run_id(process_id)

    def create_timestamp(self):
        time = super().create_timestamp()
        #time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        return time
    
    def Select_All(self):
        return super().Select_All()

    def Select_Opportunities_Started(self, process_id, date=None, last_hour=False):
        """
        Selects items that have been started but not completed
        """
        timestamp = self.create_timestamp()
        if not date:
            date, time = timestamp.split(" ")
    
        year, month, day = date.split("-")
        opp_cols = ['Date_Logged','Process_Detail_Identifier', 'Process_Detail_Status']
        where_clause = f"(DATEPART(yy, Date_Logged) = {year} AND DATEPART(mm, Date_Logged) = {month} AND DATEPART(dd, Date_Logged) = {day}) and Process_Detail_Status = 'I'"
        df = super().Select_Extra_Identifier(process_id, opp_cols, where_clause)
        opp_table = list(df['Process_Detail_Identifier'])

        return opp_table

    def Select_Opportunities_Completed(self, process_id, date=None, last_hour=False):
        """
        Based on process ID returns a list of opportunities processed by bot today,
        if date is given then will use that date instead
        IF PROVIDING DATE USE THIS FORMAT : yyyy-mm-dd
        *IF you want completed on day and items completed in the last hour, add the last hour flag to True*
        """
        timestamp = self.create_timestamp()
        print(f"Pulling Data for {process_id}")
        if not date:
            date, time = timestamp.split(" ")
    
        year, month, day = date.split("-")

        opp_cols = ['Date_Logged','Process_Detail_Identifier', 'Process_Detail_Status']
        where_clause = f"(DATEPART(yy, Date_Logged) = {year} AND DATEPART(mm, Date_Logged) = {month} AND DATEPART(dd, Date_Logged) = {day}) and Process_Detail_Status = 'S'"
        df = super().Select_Extra_Identifier(process_id, opp_cols, where_clause)

        opp_table = list(df['Process_Detail_Identifier'])
        
        if last_hour:
            where_clause = f"Date_Logged >= DATEADD(hour, -1, '{timestamp}') AND Process_Detail_Status = 'S'"
            df = super().Select_Extra_Identifier(process_id, opp_cols, where_clause)
            return opp_table, list(df['Process_Detail_Identifier'])

        return opp_table

    def complete_opportunity(self, process_id, process_run_id, identifier, update_string=None):
        """
        Marks opportunity as complete inside of the bot logs - 
        If string is given this will be sent instead
        """
        if not update_string:
            update_string = 'Completed'

        update_dict = {
            'Process_Detail_Status': 'S',
            'Process_Detail_Log': update_string
        }
        print(update_dict)
        whereclause = f"WHERE Process_Id = {process_id} AND Process_Detail_Identifier = '{identifier}'"
        data = super().Update_extra_identifier(whereclause, update_dict)
        return True 

    def update_log(self, process_id, process_run_id, identifier, log, create_duplicate):
        """
        Updates bot logs inside of the process detail table
        """
        update_dict = {
            'Date_Logged': self.create_timestamp(),
            'Process_Detail_Log': log
        }
        already_uploaded = self.create_entry(process_id, process_run_id, identifier, create_duplicate)
        if already_uploaded == True:
            return True

        if not create_duplicate:
            whereclause = f"WHERE Process_Id = {process_id} AND Process_Detail_Identifier = '{identifier}'"
        else:
            whereclause = f"WHERE Process_Id = {process_id} AND Process_Detail_Identifier = '{identifier}' AND Process_Run_Id = {process_run_id}"

        data = super().Update_extra_identifier(whereclause, update_dict)
        return False
    
    def create_entry(self, process_id, process_run_id, identifier, create_duplicate):
        """
        Using an identifier, creates an entry for the process
        identifier is expected to be an oppportunity ID
        """
        columns = ['Process_Detail_Id', 'Process_Id', 'Process_Run_Id', 'Date_Logged', 'Process_Detail_Identifier', 'Process_Detail_Status', 'Process_Detail_Log']
        where_clause = f"WHERE Process_id = {process_id}"
        table = super().Select(where_clause, columns)
        listvals_id = list(table['Process_Id'])
        # quit()
        listvals_identifier = list(table['Process_Detail_Identifier'])

        #check if the process has already created / uploaded the opportunity
        if process_id in listvals_id and identifier in listvals_identifier and not create_duplicate:
            print("Item already in details table")
            value_index = listvals_identifier.index(identifier)
            listvals_status = list(table['Process_Detail_Status'])
            if listvals_status[value_index] == 'S':
                return True

            return False

        insert_dict = {
            'Process_Id': process_id,
            'Process_Run_Id': process_run_id,
            'Date_Logged': self.create_timestamp(),
            'Process_Detail_Identifier': identifier,
            'Process_Detail_Status': "I"
        }
        data = super().Insert(insert_dict)
        
        return False
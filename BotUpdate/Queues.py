from BotUpdate.RPATable import RPATable
from datetime import datetime 
import pandas as pd

class RPA_Queue_Table(RPATable):
    def __init__(self):
        print("Initializing RPA Queue Table")
        self.table = "Queue_Item"
        self.schema = "rpa"
        super().__init__(self.schema, self.table)

    def get_timestamp(self):
        return super().create_timestamp()

    def build_update_string_none(value_dictionary):
        """
        Helper method
        Builds the update string from a dictionary value
        """
        return super().build_update_string(value_dictionary)

    def set_running_status(self, queue_item_id, status=False, queue_id=None):
        """
        Sets the main running status based on given status, True or False
        takes queue item id and returns queue id
        """
        if not queue_id:
            where_clause = f"WHERE Queue_Item_Id = '{queue_item_id}'"
            cols = ['Queue_Id']

            data = super().Select(where_clause, cols)
            queue_id = list(data['Queue_Id'])[0]

        value_dictionary = {
            'Is_Running': status
        }
        where_clause = f"WHERE Queue_Id = '{queue_id}'"
        query = f"""
        Update f
        SET f.Is_Running = '{status}'
        from {self.schema}.Queue f
        {where_clause}
        """

        data = super().Execute(query)

        return data

    def get_queue_id(self, queue_name, active_lock=None, active_lock_time=None):
        """
        Gets Queue Id back using queue Name
        """
        where_clause = f"WHERE Queue_Name = '{queue_name}'"
        query = f"""
        SELECT Queue_Id
        FROM {self.schema}.Queue
        {where_clause}
        """
        data = super().Read(query)

        if len(data) == 0:
            query = f"""
            INSERT INTO {self.schema}.Queue (Queue_Name)
            VALUES ('{queue_name}')
            """
            data = super().Execute(query)
            data = data.iloc[0,0]

            data = str(data)
            if "." in data:
                data = data.split(".")[0]
        else:
            data = list(data['Queue_Id'])[0]
            
        return data

    def add_queue(self, process_name, item_identifier, queue_id=None, status='Not Started', priority=0, sla_date=None, active_lock=None, active_lock_time=None, active_lock_name=None):
        """
        Adds items to rpa.queues
        Required: 
            Process Name - the process picking the job
            Item identifier - identifier for the item, typically the opportunity ID, this is important for detail level logging - RELATES TO QUEUE ITEM
        Other:
            data - additional data that this item has / needs, shown in json format
            status - give current status of item, defaults to not started
            priority - Priority of item, defaults to 0, 1 marks priority item
            sla_date - expected delivery, defaults to tomorrow
            active_lock - choose to not start an item , give a time and date along with it
        """
        primary_key = self.get_queue_id(process_name, active_lock, active_lock_time)
        value_dictionary = {
            'Queue_Id': primary_key,
            'Process_Name': process_name,
            'Queue_Data': item_identifier,
            'Status': status,
            'Priority': priority
        }
        #if data:
        #    value_dictionary['Data'] = data
        
        if sla_date:
            value_dictionary['SLA_Datetime'] = sla_date

        """
        if active_lock:
            value_dictionary['Active_Lock'] = active_lock
            value_dictionary['Active_Lock_Time'] = active_lock_time
            value_dictionary['Active_Lock_Name'] = active_lock_name
        """
        data = super().Insert(value_dictionary)
        
        return True

    def get_queue(self, process_name):
        """
        Gets next item in the queue by given process name, priority first, excluse active lock
        if process name not provided, return any but order by priority
        """
        queue_id = self.get_queue_id(process_name)
        where_clause = f"WHERE Queue_Id = '{queue_id}' and Status = 'Not Started' order by Priority desc" #and Active_Lock = 0"
        cols = ['*']

        data = super().Select(where_clause, cols)
        return data 

    def start_item(self, queue_item_id):
        """
        Updates item in the queue table to mark as started, sets running indicator on main queue
        """
        value_dictionary = {
            'Attempt': 1,
            #'Running': 'True',
            'Status': 'Started',
            'Last_Updated': self.get_timestamp()
        }
        where_clause = f"WHERE Queue_Item_Id = '{queue_item_id}'"
        data = super().Update_extra_identifier(where_clause, value_dictionary)

        self.set_running_status(queue_item_id, True)
        return data

    def finish_item(self, queue_item_id):
        """
        Marks an item as finished in SQL Table, and removes is running indicator from main queue
        """
        value_dictionary = {
            #'Running': 'False',
            'Status': 'Finished',
            'Completed': self.get_timestamp(),
            'Last_Updated': self.get_timestamp()
        }
        where_clause = f"WHERE Queue_Item_Id = '{queue_item_id}'"
        data = super().Update_extra_identifier(where_clause, value_dictionary)

        self.set_running_status(queue_item_id=queue_item_id, status=False)

        return data

    def reset_item(self, queue_item_id):
        """
        Resets item to not started, currently not in use
        """
        value_dictionary = {
            #'Running': 'RETRY',
            'Status': 'Not Started',
            'Last_Updated': self.get_timestamp()
        }
        where_clause = f"WHERE Queue_Item_Id = '{queue_item_id}'"
        data = super().Update_extra_identifier(where_clause, value_dictionary)

        return data

    def mark_failed(self, queue_item_id, message=None):
        value_dictionary = {
            #'Running': 'Failed',
            'Last_Updated': self.get_timestamp(),
            'Exception': self.get_timestamp(),
            'Exception_Reason': str(message)
        }
        where_clause = f"WHERE Queue_Item_Id = '{queue_item_id}'"
        data = super().Update_extra_identifier(where_clause, value_dictionary)
        
        self.set_running_status(queue_item_id=queue_item_id, status=False)
        return data


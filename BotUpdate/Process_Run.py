from BotUpdate.RPATable import RPATable
from datetime import datetime

class RPA_ProcessRun_Table(RPATable):
    """
    Class connecting to the Process run table
    This table will track individual total runs, at the start when registering the bot we expect the queue to be sent to register bot
    After finished, we expect "edit end" to be used in order to set an end time and give a finished process run
    """
    def __init__(self):
        print("Initializing RPA Process Detail Table")
        self.table = "Process_Run"
        self.schema = "rpa"
        super().__init__(self.schema, self.table)

    def get_process_id(self, process_name):
        return super().get_process_id(process_name)

    def create_timestamp(self):
        time = super().create_timestamp()
        #time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        return time

    def get_process_run_id(self, process_id):
        """
        Gets a process run id from the process run table
        while using the process id from rpa process table
        """
        return super().get_process_run_id(process_id)
    
    def create_entry(self, process_id):
        """
        Checks if entry is already created inside table
        if created, returns process run id
        if not, creates entry with process id and returns process run id
        """
        select_list = ['Process_Run_Id']
        
        insert_dict = {
            'Process_Id' : process_id,
            'Process_Run_Status': 'I'
        }

        data = super().Insert(insert_dict)

        data = data.iloc[0].to_string()
        data = data.split(".")[0]
        data = int(data)

        self.edit_start(data)
        return data

    def edit_start(self, process_run_id, start_time=None):
        """
        Edits the start time in the process run table
        creates timestamp if not given
        """
        if not start_time:
            start_time = self.create_timestamp()

        update_dict = {
            'Start_Time': start_time,
            'Is_Running': 1,
        }

        return super().Update_Process_run(process_run_id, update_dict)

    def edit_end(self, process_run_id, queue_amount, end_time=None):
        """
        Edits the start end in the process run table
        creates timestamp if not given
        """
        if not end_time:
            end_time = self.create_timestamp()
            
        update_dict = {
            'End_Time': end_time,
            'Is_Running': 0,
            'Process_Run_Status': "S",
            'Process_Run_Log': f'Completed: {queue_amount}' 
        }
        return super().Update_Process_run(process_run_id, update_dict)

    def edit_log(self, process_run_id, process_run_log):
        """
        Edits the process run log field
        expected: 
        In Queue: x
        OR
        Completed: x
        """
        update_dict = {
            'Process_Run_Log': process_run_log
        }
        return super().Update_Process_run(process_run_id, update_dict)

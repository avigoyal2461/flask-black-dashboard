from BotUpdate.RPATable import RPATable
from BotUpdate.Process_Run import RPA_ProcessRun_Table
from BotUpdate.Process_Detail import RPA_ProcessDetail_Table
from BotUpdate.Queues import RPA_Queue_Table

class RPA_Process_Table(RPATable):
    """
    REGISTER BOT: will create an entry in the process run, uses identifier or comes up default, we can upload logs to this table at any point with register
    this will also create an entry in the process table if not already existing
    UPDATE BOT STATUS: will update status in process table for the time, then update the process detail table to add details of current status
    
    Main method: update_bot_status - takes process name and a status, time is not required as we will make a timestamp
    the rest are helper methods
    """
    def __init__(self):
        print("Initializing RPA Process Table")
        self.table = "Process"
        self.schema = "rpa"
        super().__init__(self.schema, self.table)
        self.process_run = RPA_ProcessRun_Table()
        self.process_detail = RPA_ProcessDetail_Table()
        self.queue_table = RPA_Queue_Table()
        self.process_run_id = 0
        self.queue_amount = ""

    def get_process_id(self, process_name):
        return super().get_process_id(process_name)

    def update_time(self, time=None, process_name=None):
        """
        Updates Last updated time in the process table
        """
        if not time:
            time = super().create_timestamp()

        if not process_name:
            return "Please Add a process_name to continue (to view table try : Select_All)"

        process_id = self.get_process_id(process_name)
        # self.connect_to_server()

        update_dict = {
            'Last_Updated_Timestamp' : time
        }

        data = super().Update(process_id, update_dict)

        return data

    def update_active(self, process_name=None, activeind=None):
        """
        Updates the Active ind of the bot, by default: 1
        """
        if not process_name:
            return "Please Add a process_name to continue (to view table try : Select_All)"

        if not activeind:
            print("Status will be defaulted to '1'")
            activeind = 1

        process_id = self.get_process_id(process_name)

        update_dict = {
            'Active_Ind': activeind
        }

        data = super().Update(process_id, update_dict)
        return data

    def register_bot(self, process_name, process_description=None, is_continuous=None, active_ind=None, logs=None):
        """
        Check if bot is in table, if not then registers
        THE ONLY REQUIRED FIELD : PROCESS_NAME
        Fields - 
        process_description - a short description on the process in the process table
        is_continuous - integer to show if the process is continuous or not(0, 1)
        active_ind - integer to show if the process is active (0, 1)
        logs - logs to show either the queue or completed Expected: "In Queue: x" OR "Completed: x"
        """

        table = self.Select_All()
        listvals = list(table['Process_Name'])
        print(f"Registering Bot {process_name}")

        #logs are for the queue, we either show how many are in the queue or say how many we 
        #checks if value is in table, if it is then we exit
        if process_name in listvals:
            print("Value has already been registered, creating run value")
            #create entry in process run table, if created then pass
            id = self.get_process_id(process_name)
            self.process_run_id = self.process_run.create_entry(id)
            #creates a start time entry
            self.process_run.edit_start(self.process_run_id)

            if logs:
                self.queue_amount = logs.split(":")[1]
                print("Added logs to process run table")
                self.process_run.edit_log(self.process_run_id, logs)

            return True
        #sets default values for anything not given
        if not process_description:
            print("Setting default description, please update later")
            process_description = "Default Description of a bot process.. Registration Complete"

        if not is_continuous:
            print("Setting default value to 1 for is_continuous")
            is_continuous = 1

        if not active_ind:
            print("Setting default value to 1 for active_ind")
            active_ind = 1

        insert_dict = {
            'Process_Name': process_name,
            'Process_Description': process_description,
            'Is_Continuous': is_continuous,
            'Active_Ind': active_ind
        }

        data = super().Insert(insert_dict)

        #create entry in process run table, if created then pass
        print("Creating Process Run Entry")
        id = self.get_process_id(process_name)
        self.process_run_id = self.process_run.create_entry(id)
        #creates a start time entry
        self.process_run.edit_start(self.process_run_id)
        
        if logs:
                self.queue_amount = logs.split(":")[1]
                print("Added logs to process run table")
                self.process_run.edit_log(self.process_run_id, logs)

        print(f"Registered Bot {process_name}")

        return data

    def update_status(self, bot_name):
        """
        Simple method to only update last updated time and not create entry in the detail table
        """
        self.update_time(time=None, process_name=bot_name)
        
        self.update_active(process_name=bot_name, activeind=1)
        print("Updated bot Status")

    def update_bot_status(self, bot_name=None, status=None, identifier=None, create_duplicate=None): #true or false to create entry if its already in table
        """
        status gets added to process detail log
        Updates Bot Status, time is generated by the method, sets active_ind to 1
        REQUIRED FIELD: BOT_NAME
        """
        print("Updating Bot Status")
        if not bot_name:
            print("Please Add a process_name to continue (to view table try : Select_All)")
            return "Please Add a process_name to continue (to view table try : Select_All)"

        if not status:
            print("Status will be defaulted to 'No Status to Display'")
            status = "No Status to Display"
        
        if not identifier:
            print("Identifier will be set to default, this will act as a overall actively updated log")
            identifier = "Default"

        self.update_time(time=None, process_name=bot_name)
        
        self.update_active(process_name=bot_name, activeind=1)

        process_id = self.get_process_id(bot_name)
        already_uploaded = self.process_detail.update_log(process_id, self.process_run_id, identifier, status, create_duplicate)
        
        print("Updated Bot Status")
        #this is a tracker object, if we have already uploaded the item into the table
        #then we continue - shows true when item is already uploaded
        return already_uploaded
    
    def update_logs(self, logs):
        """
        Updates a bot's logs in the process run table
        """
        self.queue_amount = logs.split(":")[1]
        print("Added logs to process run table")
        self.process_run.edit_log(self.process_run_id, logs)

        return True

    def complete_opportunity(self, bot_name, identifier, update_string=None, queue_item_id=None):
        """
        Marks a Single opportunity as finished in the process detail table
        """
        process_id = self.get_process_id(bot_name)

        self.process_detail.complete_opportunity(process_id, self.process_run_id, identifier, update_string)
        if queue_item_id:
            self.queue_table.finish_item(queue_item_id)
        

    def edit_end(self, time=None):
        """
        Calls the process_run table to edit the end time
        This method is not being used by anything, but can be used
        to form a connection with the end time wihtout importing
        the run table 
        NEEDS TO BE CALLED TO CREATE AN END TIME IN THE TABLE
        """
        if not time:
            self.process_run.edit_end(self.process_run_id, self.queue_amount, end_time=None)
        else:
            self.process_run.edit_end(self.process_run_id, self.queue_amount, time)

        print("Successfully Ended the process")
        return

    def Select_All(self):
        return super().Select_All()

    def Select_Opportunities_Started(self, bot_name, last_hour=False):
        process_id = self.get_process_id(bot_name)
        if last_hour:
            data = self.process_detail.Select_Opportunities_Started(process_id, last_hour=True)
        else:
            data = self.process_detail.Select_Opportunities_Started(process_id)
        return data
    
    def Select_Opportunities_Completed_Today(self, bot_name, last_hour=False):
        process_id = self.get_process_id(bot_name)
        if last_hour:
            data = self.process_detail.Select_Opportunities_Completed(process_id, last_hour=True)
        else:
            data = self.process_detail.Select_Opportunities_Completed(process_id)
        return data
        
if __name__ == "__main__":
    a = RPA_Process_Table()
    # a.test_log(11, '0065b00000yS6SN', 'Completed')
    a.register_bot("bot_test")
    a.update_bot_status("bot_test", "status", "ident2")
    a.complete_opportunity("bot_test", "ident2", "test")
    a.edit_end()

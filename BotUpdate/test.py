import os
import sys
sys.path.append(os.environ['autobot_modules'])
from BotUpdate.Queues import RPA_Queue_Table

if __name__ == "__main__":
    a = RPA_Queue_Table()
    print(a.mark_failed(6, "User was not found in spotio"))
    #item = '01248714124'
    #a.finish_item('test', item)
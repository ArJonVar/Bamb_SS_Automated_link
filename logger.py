from datetime import datetime
import os
import sys
import inspect

class ghetto_logger:
    '''to deploy in class, put self.log=ghetto_logger("<module name>.py"), then ctr f and replace print( w/ self.log.log('''
    def __init__(self, title, debug = False):
        raw_now = datetime.now()
        self.debug= debug
        self.now = raw_now.strftime("%m/%d/%Y %H:%M:%S")
        self.first_use=True
        self.first_line_stamp  = f"{self.now}  {title}--"
        if os.name == 'nt':
            self.path ="C:\Egnyte\Private\cobyvardy\Other_Projects\Python\Ariel\Bamb_SS_Automated_Link\deployment_logger.txt"
        else:
            self.path ="av_logger.txt"

    def log(self, text, type = "new_line", mode="a"):
        function_name = inspect.currentframe().f_back.f_code.co_name
        
        try:
            module_name = inspect.getmodule(inspect.stack()[1][0]).__name__
        except:
            module_name = "__main__"

        func_stamp = f"{module_name}.{function_name}(): "

        with open(self.path, mode=mode) as file:
            if self.first_use == True:
                file.write("\n" + "\n"+ self.first_line_stamp)
                self.first_use = False
            if self.first_use == False and type == "paragraph":
                file.write(text)
            elif self.first_use == False:
                file.write("\n  " + func_stamp + text)
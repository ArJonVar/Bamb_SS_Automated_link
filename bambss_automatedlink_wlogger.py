#region imports
import smartsheet
from PyBambooHR import PyBambooHR
import time
import datetime
import pandas as pd
from smartsheet_grid import grid
from globals import sensative_smartsheet_token, sensative_bamboo_token
from logger import ghetto_logger
#endregion

class BambSSLink:
    def __init__(self, config):
        self.config=config
        self.smart = smartsheet.Smartsheet(self.config.get("stoken"))
        self.smart.errors_as_exceptions(True)
        self.bamboo = PyBambooHR.PyBambooHR(subdomain="dowbuilt", api_key=self.config.get("btoken"))
        grid.token=self.config.get("stoken")
        self.dest_sheet_id = config.get("dest_sheet_id")
        self.log=ghetto_logger("bambss_automatedlink_wlogger.py")
        self.sheet_id_to_full_dict = lambda sheet_id: self.smart.Sheets.get_columns(sheet_id,level=2).to_dict()
        self.dest_sheet_df, self.dest = self.fetch_df(self.dest_sheet_id)
        self.start_time = time.time()
        self.gather_column_ids()
    # region data gather/preprocessing
    def fetch_df(self, sheet_id):
        '''fetches data from smartsheet, and returns row data'''
        df=grid(sheet_id)
        df.fetch_content()
        df_obj = df.df
        return df, df_obj
    def fetch_column_id(self, str):
        '''fetches a column id (by matching column name to sheet data) with a try/except so it can log what went wrong'''
        ddf = self.dest_sheet_df.column_df
        try:
            column_id = ddf.loc[ddf['title'] == str]['id'].tolist()[0]
            return column_id
        except IndexError:
            self.log.log(f"failed to find column_id for {str}, check that the column names have not changed and try again")
    def gather_column_ids(self):
        '''gathers column ids from current destination sheet id (incase we want to switch out the sheet)'''
        self.columnid_sage_id = self.fetch_column_id("sage_id")
        self.columnid_fullname = self.fetch_column_id("fullName")
        self.columnid_firstname = self.fetch_column_id("firstName")
        self.columnid_preferredname = self.fetch_column_id("preferredName")
        self.columnid_jobtitle = self.fetch_column_id("jobTitle")
        self.columnid_department = self.fetch_column_id("department")
        self.columnid_location = self.fetch_column_id("location")
        self.columnid_division = self.fetch_column_id("division")
        self.columnid_supervisor = self.fetch_column_id("supervisor")
        self.columnid_bamboo_id = self.fetch_column_id("Bamboo_id")
        self.columnid_mobilephone = self.fetch_column_id("mobilePhone")
        self.columnid_workemail = self.fetch_column_id("workEmail")
        self.columnid_employee_number = self.fetch_column_id("employee_number")
        self.columnid_workphone = self.fetch_column_id("workPhone")
        self.columnid_photoUrl = self.fetch_column_id("photoUrl")
    def fetch_sheet_grid_obj(self, sheet_id):
        df=grid(sheet_id)
        df.fetch_content()
        return df
    def timestamp(self): 
        '''creates a string of minute/second from start_time until now for logging'''
        end_time = time.time()  # get the end time of the program
        elapsed_time = end_time - self.start_time  # calculate the elapsed time in seconds       

        minutes, seconds = divmod(elapsed_time, 60)  # convert to minutes and seconds       
        timestamp = "{:02d}:{:02d}".format(int(minutes), int(seconds))
        
        return timestamp
    # endregion
    # region data processing
    def fetch_dir_df(self):
        '''gets the full bamboo df'''
        dir = self.bamboo.get_employee_directory()
        dir_df = pd.DataFrame(dir)
        return dir_df   
    
    def add_sageids(self):
        '''the df comes with default options, the first non default option we need to add is sage/employee numbers'''
        sageids = [
            self.bamboo.get_employee(id, field_list=["customSageID"]).get("customSageID")
            for id in self.dir_df_raw["id"]
        ]

        self.dir_df_raw["sage_id"] = sageids
        self.log.log(f"{self.timestamp()}  sageids imported from Bamboo API")    
    def add_employee_ids(self):
        '''the df comes with default options, the first non default option we need to add is employee numbers'''
        employee_numbers = [
            self.bamboo.get_employee(id, field_list = ['employeeNumber']).get('employeeNumber') 
            for id in self.dir_df_raw['id']
            ]
        
        self.dir_df_raw["employee_number"] = employee_numbers
        self.log.log(f"{self.timestamp()}  employee numbers imported from Bamboo API")  

    def add_preferred_name(self):
        '''the def comes with default options, now we add preffered name as a new column'''
    
        self.dir_df_raw["fullName"] = self.dir_df_raw.apply(self.preffered_name_logic, axis=1)  
    def preffered_name_logic(self, dir_df):
        '''figured out what the preffered name should be based on simple logic'''
        if dir_df["preferredName"] == None:
            return dir_df["firstName"] + " " + dir_df["lastName"]
        else:
            return dir_df["preferredName"] + " " + dir_df["lastName"] 

    def reorder_df(self):
        '''rename id bamboo id & reorder'''
        self.dir_df_raw.rename(columns={"id": "Bamboo_id"}, inplace=True)
        self.dir_df = self.dir_df_raw[
           [
               "sage_id",
               "fullName",
               "firstName",
               "lastName",
               "preferredName",
               "jobTitle",
               "department",
               "location",
            #    "division",
               "supervisor",
               "Bamboo_id",
               "mobilePhone",
               "workEmail",
               "employee_number",
               "workPhone",
               "photoUrl",
           ]
        ]
        
        self.log.log(f"{self.timestamp()}  Bamboo data-transformation complete") 
    # endregion
    # region SS post
    def delete_rows(self):
        '''deletes sheet to prep for reposting'''
        self.log.log(f"{self.timestamp()}  Deleting all rows in Smartsheet...")
        row_list_del = []
        for rowid in self.dest_sheet_df.df['id'].to_list():
            row_list_del.append(rowid)
            # Delete rows to sheet by chunks of 200
            if len(row_list_del) > 199:
                self.smart.Sheets.delete_rows(self.dest_sheet_id, row_list_del)
                row_list_del = []
        # Delete remaining rows
        if len(row_list_del) > 0:
            self.smart.Sheets.delete_rows(self.dest_sheet_id, row_list_del)
    def post_update(self):
        '''this is when data is posted to the smartsheet'''
        self.smart_rows = []
        values_list = self.dir_df.values.tolist()
        for row, rownum in zip(values_list, range(len(values_list))):
            smart_row = self.smart.models.Row()
            smart_row.to_bottom = True
            for item, column in zip(row, self.dest_sheet_df.grid_column_ids):
                if item == None or str(item) == 'nan':
                    item = ""
                if column == self.columnid_workemail:
                    nme = self.dir_df.fullName[rownum]
                    email = item
                    if email in (None, ""):
                        email = f"{self.dir_df.lastName.values[rownum]}.field-employee@dowbuilt.com"
                        email = email.replace(" ", "_")
                    if nme == None:
                        nme = item
                    item_contact = {
                        "objectType": "MULTI_CONTACT",
                        "values": [{"email": email, "name": nme, "objectType": "CONTACT"}],
                    }
                    item_dict = {}
                    item_dict["object_value"] = item_contact
                    item_dict["column_id"] = column
                    item_dict["strict"] = False
                    # self.log.log(item_dict)
                    smart_row.cells.append(item_dict)
                else:
                    smart_row.cells.append(
                        {"column_id": column, "value": item, "strict": False}
                    )
            self.smart_rows.append(smart_row)

        self.response = self.smart.Sheets.add_rows(self.dest_sheet_id, self.smart_rows)

        self.log.log(f"{self.timestamp()}  Employees_Bamboo Smartsheet Rows Updated")
    def rename_ss(self):
        '''changes the sheet's name to the datetime the update happened'''
        name = "Employees_Bamboo"
        now = datetime.datetime.now()
        dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
        self.update_name = name + " updated:" + dt_string
        update_data = self.smart.models.Sheet({"name": self.update_name})

        update_response = self.smart.Sheets.update_sheet(self.dest_sheet_id, update_data)
        self.log.log(f"{self.timestamp()}  {name} Sheet Name Updated")
    # endregion
    def cron_run(self):
        '''executes the main script'''
        self.log.log(f'{self.timestamp()} start')
        self.dir_df_raw = self.fetch_dir_df()
        self.add_sageids()
        self.add_employee_ids()
        self.add_preferred_name()
        self.reorder_df()
        self.delete_rows()
        self.post_update()
        self.rename_ss()
        self.log.log(f'{self.timestamp()} fin')

if __name__ == "__main__":
    config = {'stoken':sensative_smartsheet_token, 'btoken':sensative_bamboo_token, 'dest_sheet_id': 5956860349048708}
    bsl = BambSSLink(config)
    bsl.cron_run()
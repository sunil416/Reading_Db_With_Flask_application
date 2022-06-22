
'''
    Author: Sunil Kumar
    File Name: DBConnector.py
    Description: Helps in DB realted CURD operation 

'''


from datetime import datetime
from distutils.command.config import config
import email
from pickle import NONE
from App_Logger.AppLogger import AppLogger
from Modules.Modules import ModuleName
from Configuration.config import Config
import sqlite3
from datetime import datetime


class dBOperation:

    def __init__(self):
        self.logger = AppLogger()
        self.config= Config()


    def dataBaseConnection(self,DatabaseName):

        try:
            conn = sqlite3.connect(self.config.base_path+DatabaseName+'.db')

            self.logger.log(ModuleName.DataBase, f"Successfully stablished the DB connection. \n")
        except ConnectionError:
            self.logger.log(ModuleName.DataBase, f"Error while Connecting the Database. \n")
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names, table_name):

        try:
            conn = self.dataBaseConnection(DatabaseName)
            query= f"DROP TABLE IF EXISTS {table_name}"
            conn.execute(query)
            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    query= f"ALTER TABLE {table_name}  ADD COLUMN {key} {type}"
                    conn.execute(query)
                except:
                    #conn.execute('CREATE TABLE  "{conf.Employe}" ({column_name} {dataType})'.format(column_name=key, dataType=type))
                    query= f"CREATE TABLE {table_name} ({key} {type})"
                    conn.execute(query)

            conn.close()

            self.logger.log(ModuleName.DataBase, f"Tables created successfully!! \n")
            self.logger.log(ModuleName.DataBase, f"Closed {DatabaseName} database successfully \n")

        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error during the connection \n")
            raise e

    def insertDataInTbale(self,DatabaseName, table_name, dataframe):
        conn = self.dataBaseConnection(DatabaseName)
        try:
            for i in range(0, dataframe.shape[0]):
                if table_name == self.config.Employe:
                    email= dataframe.iloc[i]["EmailId"]
                    hire_date=dataframe.iloc[i]["Hire_Date"]
                    City=dataframe.iloc[i]["City"]
                    query=f"INSERT INTO {table_name} VALUES('{email}' , '{hire_date}' ,'{City}' )"
                    conn.execute(query)
                elif table_name == self.config.Hike:
                    Year= dataframe.iloc[i]["Year"]
                    Pay_Raise=dataframe.iloc[i]["Pay_Raise"]
                    query=f"INSERT INTO {table_name} VALUES('{Year}' , '{Pay_Raise}' )"
                    conn.execute(query)
                elif table_name== self.config.EmployeDetails:
                    EmailId= dataframe.iloc[i]["EmailId"]
                    Emp_id=dataframe.iloc[i]["Emp_id"]
                    first_name= ""
                    last_name=""
                    HireDate= datetime.min
                    City=""
                    HiringYear=0
                    Payroll=dataframe.iloc[i]["Payroll"]
                    PayRoleRisePercentage=0
                    RaisePayroll=0
                    query=f"INSERT INTO {table_name} VALUES('{EmailId}' , '{Emp_id}', '{first_name}', '{last_name}',  '{HireDate}', '{City}', '{HiringYear}', '{Payroll}', '{PayRoleRisePercentage}', '{RaisePayroll}' )"
                    conn.execute(query)
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
        conn.commit()
        conn.close()

    def readData(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name};"
            curso.execute(query)
            return curso.fetchall()
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e

    def updateFirstNameAndLastNameBasedOnEmail(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name};"
            curso.execute(query)
            data= curso.fetchall()
            for item in data:
                EmailId= item[0]
                names=EmailId.split('@')[0].split('.')
                first_name= names[0]
                last_name=names[1]
                query=f"UPDATE {table_name} SET First_Name = '{first_name}', Last_Name = '{last_name}' WHERE Email = '{EmailId}'"
                conn.execute(query)
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
        conn.commit()
        conn.close()

    def updateCity(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name};"
            curso.execute(query)
            data= curso.fetchall()
            for item in data:
                EmailId= item[0]
                query =f"SELECT CITY from {self.config.Employe} where Email = '{EmailId}'"
                curso.execute(query)
                city =curso.fetchone()
                if city == None:
                    query=f"UPDATE {table_name} SET City = 'London' WHERE Email = '{EmailId}'"
                else:
                    query=f"UPDATE {table_name} SET City = '{city[0]}' WHERE Email = '{EmailId}'"
                conn.execute(query)
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
        conn.commit()
        conn.close()

    def updateHireDate(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name};"
            curso.execute(query)
            data= curso.fetchall()
            for item in data:
                EmailId= item[0]
                query =f"SELECT HireDate from {self.config.Employe} where Email = '{EmailId}'"
                curso.execute(query)
                HireDate =curso.fetchone()
                if HireDate == None:
                    query=f"UPDATE {table_name} SET HireDate = '1/5/2020', HiringYear ='2020' WHERE Email = '{EmailId}'"
                else:
                    da=datetime.strptime(HireDate[0], "%Y-%m-%d %H:%M:%S")
                    query=f"UPDATE {table_name} SET HireDate = '{da}', HiringYear= '{da.year}' WHERE Email = '{EmailId}'"
                conn.execute(query)
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
        conn.commit()
        conn.close()

    def checkEmployeIsExperience(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name};"
            curso.execute(query)
            data= curso.fetchall()
            dict={}
            for item in data:
                hiring_year=item[6]
                email_id=item[0]
                if hiring_year<2019:
                    dict.update({email_id:"Experience"})
                else:
                    dict.update({email_id:"No experience"})
            conn.close()
            return dict
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
    
    def updateAppraisal(self,DatabaseName, table_name):
        
        try:
            conn = self.dataBaseConnection(DatabaseName)
            curso= conn.cursor()
            query= f"SELECT * from {table_name} order by Year DESC;"
            curso.execute(query)
            data= curso.fetchone()
            hike_rate=data[1]/100
            query = f"SELECT Email, Payroll from {self.config.EmployeDetails}"
            data= curso.execute(query)

            for item in data:
                email_id= item[0]
                pay= item[1]
                new_pay=round(pay*(1+hike_rate))
                change_rate= hike_rate
                query= f"UPDATE {self.config.EmployeDetails} SET RaisePayroll = {new_pay} , PayRoleRisePercentage = {change_rate} where Email = '{email_id}'"
                conn.execute(query)
            conn.commit()
            conn.close()
            return dict
        except Exception as e:
            self.logger.log(ModuleName.DataBase, f"Error insterting the data \n")
            conn.close()
            raise e
        
    

    


   




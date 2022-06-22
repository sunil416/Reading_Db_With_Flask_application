import os
from pathlib import Path
from flask import Flask
import json
from App_Logger.AppLogger import AppLogger
from Configuration.config import Config
from DBConnector.DBConnector import  dBOperation
from Modules.Modules import ModuleName
from flask_debugtoolbar import DebugToolbarExtension
import pandas as pd


app = Flask(__name__)
app.debug = True
app.secret_key = 'development key'

#toolbar = DebugToolbarExtension(app)

#@app.route("/")
def home():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()
      
    
    dbclient.createTableDb(conf.database,read_Schema(logger,conf,conf.Employe), conf.Employe )
    dbclient.insertDataInTbale(conf.database, conf.Employe,read_data_to_insert(logger, conf, conf.Employe))

    dbclient.createTableDb(conf.database,read_Schema(logger,conf,conf.Hike), conf.Hike )
    dbclient.insertDataInTbale(conf.database, conf.Hike,read_data_to_insert(logger, conf, conf.Hike))

    dbclient.createTableDb(conf.database,read_Schema(logger,conf,conf.EmployeDetails), conf.EmployeDetails )
    dbclient.insertDataInTbale(conf.database, conf.EmployeDetails,read_data_to_insert(logger, conf, conf.EmployeDetails))
    #data=dbclient.fetch_all_data_from_collection()
    # for item in data:
    #     print(item)
    print( "Base Infra is created")

def ReadData():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()

    print(dbclient.readData(conf.database, conf.Employe))

    print(f"\n {dbclient.readData(conf.database, conf.Hike)}")

    print(f"\n {dbclient.readData(conf.database, conf.EmployeDetails)}")

def updateFirstAndLastname():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()


    dbclient.updateFirstNameAndLastNameBasedOnEmail(conf.database, conf.EmployeDetails)

def updateCity():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()


    dbclient.updateCity(conf.database, conf.EmployeDetails)

def updateHireDate():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()


    dbclient.updateHireDate(conf.database, conf.EmployeDetails)


def checkEmployeIsExperience():

    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()


    print(dbclient.checkEmployeIsExperience(conf.database, conf.EmployeDetails))

def read_data_to_insert(logger, config, table_name):
    try:
        schema_path=Path(f"{config.base_path}\\Data\\{table_name}.xlsx")
        return pd.read_excel(schema_path)

    except Exception as e:
        logger.log(ModuleName.General, f"Error Reading the data file {table_name}") 

def read_Schema(logger, config, schema_name):
    try:
        schema_path=Path(f"{config.base_path}\\Models\\{schema_name}.json")
        with open(schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
                return dic["ColName"]
    except Exception as e:
        logger.log(ModuleName.General, "Error Reading the schema file")

def updateApprasial():
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()


    dbclient.updateAppraisal(conf.database, conf.Hike)

def readTable(tableName):
    logger=AppLogger()
    logger.log(ModuleName.General, "Started With home route")
    dbclient=dBOperation()
    conf=Config()
    tdata=dbclient.readData(conf.database, tableName)
    for item in tdata:
        tab=""
        for col in item:
            tab+=f"{col} \t"
        print(tab)


if __name__ == "__main__":
    conf=Config()
    print(f"\n Welecome to the system \n Please Choose from belwo options")
    while(True):
        
        print("\n1. Create a basic Structure of the database")
        print("2. Update the First Name and Last Name of the employee")
        print("3. Update the City of the employee")
        print("4. Update the Hire Date of the employee")
        print("5. Check the employee is experience or not")
        print("6. Calculate the Pay Raise")
        print("7. View Employee Old Table")
        print("8. View Employee New Table")
        print("9. View Hike Table")
        print("10. Exit")

        option =input("\n Please Choose your option between 1 to 10: ")
        try:
            option_int = int(option)
            if option_int ==1:
                home()
            elif option_int ==2:
                updateFirstAndLastname()
            elif option_int ==3:
                updateCity()
            elif option_int ==4:
                updateHireDate()
            elif option_int ==5:
                checkEmployeIsExperience()
            elif option_int ==6:
                updateApprasial()
            elif option_int ==7:
                readTable(conf.Employe)
            elif option_int ==8:
                readTable(conf.EmployeDetails) 
            elif option_int ==9:
                readTable(conf.Hike) 
            elif option_int ==10:
                break
        except :
            print("Wrong input, please try again")

    #app.run(debug=True)
    
import os


class Config:

    def __init__(self) -> None:
        self.userName="sunil416"
        self.password="anil"
        self.database='Company'
        self.EmployeDetails="EmployeDetails"
        self.Employe="Employe"
        self.Hike="Hike"
        self.base_path= os.getcwd()
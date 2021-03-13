from termcolor import colored
from main import *

colors = ["green", "blue", "yellow", "red", "cyan"]


# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480,
def banner():
    print(colored('''
    @============================================================================================@

     /$$   /$$ /$$   /$$                         /$$                 /$$
    | $$  | $$| $$  | $$                        | $$                | $$
    | $$  | $$| $$  | $$ /$$  /$$  /$$  /$$$$$$ | $$$$$$$   /$$$$$$$| $$$$$$$   /$$$$$$   /$$$$$$
    | $$$$$$$$| $$  | $$| $$ | $$ | $$ /$$__  $$| $$__  $$ /$$_____/| $$__  $$ /$$__  $$ /$$__  $$
    | $$__  $$| $$  | $$| $$ | $$ | $$| $$$$$$$$| $$  \ $$|  $$$$$$ | $$  \ $$| $$  \ $$| $$  \ $$
    | $$  | $$| $$  | $$| $$ | $$ | $$| $$_____/| $$  | $$ \____  $$| $$  | $$| $$  | $$| $$  | $$
    | $$  | $$|  $$$$$$/|  $$$$$/$$$$/|  $$$$$$$| $$$$$$$/ /$$$$$$$/| $$  | $$|  $$$$$$/| $$$$$$$/
    |__/  |__/ \______/  \_____/\___/  \_______/|_______/ |_______/ |__/  |__/ \______/ | $$____/
                                                                                        | $$
                                                                                        | $$
                                                                                        |__/

    @============================================================================================@

    ''', colors[randrange(5)]))


# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480,
def mysql_rules():
    print(colored('''
    @=============================== MySQL database regels (voorbeeld) ==========================@\n
    1: products (table) | (Columns): ()
    2: profiles (table) | (Columns): ()
    3: sessions (table) | (Columns): ()\n
    @============================================================================================@\n
    ''', colors[randrange(2)]))


# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480,
def opties():
    print(colored('''
    @====================================== Menu opties =========================================@\n
    1: Automatisch aanmaken van het predifined mySQL tables
    2: Overzetten van big data vanuit (mongoDB) naar (mySQL)
    3: Verwijderen van mySQL database table
    4: Error menu
    5: Quit program\n
    @============================================================================================@\n
    ''', colors[randrange(2)]))


# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480,
def table_remove_options():

    print(colored('''
    @====================================== Delete Table Options ================================@\n
    1: Verwijder alle tables
    2: Verwijder geselecteerde table\n
    @============================================================================================@\n
    ''', colors[randrange(3)]))


# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480, Kenny van den Berg, Studentnummer: 1777503
def menu():
    """
    Roep de banner en het optie menu op. Als optie input 1 is start function create_tables_process.
    Als optie input 2 is word de functie  transfer_data_process  op geroepen.
    Als optie input 3 is word de functie  delete_database op geroepen.
    Als optie input 4 is word de lijst aan errors getoont.
    Als optie input 5 stopt het program
    :return:
    """

    banner()
    options = ["1", "2", "3", "4", "5"]
    error_list = []

    while True:
        try:
            opties()
            optie = ""
            while optie not in options:
                optie = input(colored("\n\tKies je optie: > ", "yellow"))
                if optie not in options:
                    print(colored("\n\tdie optie bestaat niet", "red"))
                    time.sleep(1)
            if optie == "1":
                create_tables_process()
            elif optie == "2":
                transfer_data_process()
            elif optie == "3":
                delete_database_process()
            elif optie == "4":
                if len(error_list) != 0:
                    for item in error_list:
                        print(item)
                time.sleep(3)
            elif optie == "5":
                print(colored("\n\tThank you and goodbye", "red"))
                time.sleep(1)
                return
            optie = 0
        except mysql.connector.Error as e:
            error_list.append(e)


# Voornamelijk gemaakt door: Kenny van den Berg, Studentnummer: 1777503, Ceyhun Cakir, Studentnummer: 1784480
def give_user_password():
    """"
    Ontvang user en passwoord.
    """
    user = input(colored("\n\tGeef je mySQL username op: > ", "yellow"))
    passwd = input(colored("\n\tGeef je mySQL password op: > ", "yellow"))
    return user, passwd


# Voornamelijk gemaakt door: Kenny van den Berg, Studentnummer: 1777503, Ceyhun Cakir, Studentnummer: 1784480
def create_tables_process():
    """
     start het create database process. vraag je om een user, password & database input, roep daarna mysql_connector() op om je aan te melden.
    make_mysql_table_process word opgeroepen uiteindelijk na het aanmeldin
     """
    user, passwd = give_user_password()
    database = input(colored("\n\tGeef je mySQL database op: > ", "yellow"))

    db, cursor = mysql_connector(user, passwd, database)
    create_predefined_tables(cursor, database)

    print(colored("\n\tTables zijn aangemaakt", "yellow"))
    sql_closer(db, cursor)


# Voornamelijk gemaakt door: Kenny van den Berg, Studentnummer: 1777503, Ceyhun Cakir, Studentnummer: 1784480
def transfer_data_process():
    """
    start het transfer database process.
    """
    mongo_db_names = mongo_database_names()
    print("\n\t", mongo_db_names)

    while True:
        database = input(colored("\n\tGeef je MongoDB database op: > ", "yellow"))
        if database not in mongo_db_names:
            print(colored("\n\tdie optie bestaat niet", "red"))
        else:
            break

    user, passwd = give_user_password()
    my_sql_database_names = get_mysql_database_names(user, passwd)
    print("\n\t", my_sql_database_names)

    while True:
        my_sql_database = input(colored("\n\tGeef je Mysql database op: > ", "yellow"))
        if my_sql_database not in my_sql_database_names:
            print(colored("\n\tdie optie bestaat niet", "red"))
        else:
            break

    print(colored("\n\tMysql tables word gevuld", "yellow"))
    db, cursor = mysql_connector(user, passwd, my_sql_database)

    delete_database(my_sql_database, cursor)
    create_predefined_tables(cursor, my_sql_database)

    end, start = get_item_from_collection(db, cursor, database)

    print(colored("\n\tMysql tables zijn gevuld", "green"))
    print("\n\tTotal time taken, " + str(round(end - start, 6)) + " seconds", "green")

    sql_closer(db, cursor)


# Voornamelijk gemaakt door: Kenny van den Berg, Studentnummer: 1777503, Ceyhun Cakir, Studentnummer: 1784480
def delete_database_process():
    """
    start het delete database process. vraag je om een user & password input en gebruik get_mysql_database_names()
    om mysql databases namen te zien. Hier na wordt de functie delete_database om het ingegeven database te verwijderen.
    """
    user, passwd = give_user_password()

    database_names = get_mysql_database_names(user, passwd)
    print(database_names)

    database = input(colored("\n\tGeef je mySQL database op: > ", "yellow"))
    db, cursor = mysql_connector(user, passwd, database)

    delete_database(database, cursor)
    sql_closer(db, cursor)


menu()

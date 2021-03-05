import pymongo
from random import *
from termcolor import colored
import sys
import mysql.connector
import time
import os
from collections import Counter

colors = ["green", "blue", "yellow", "red", "cyan"]

# 3 forloops voor get collection
# 3 forloops voor insert mysql database

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
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

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def mysql_rules():
    print(colored('''
    @=============================== MySQL database regels (voorbeeld) ==========================@\n
    1: products (table) | (Columns): ()
    2: profiles (table) | (Columns): ()
    3: sessions (table) | (Columns): ()\n
    @============================================================================================@\n
    ''', colors[randrange(2)]))

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def opties():
    print(colored('''
    @====================================== Menu opties =========================================@\n
    1: Automatisch aanmaken van het predifined mySQL tables
    2: Overzetten van big data vanuit (mongoDB) naar (mySQL)
    3: Verwijderen van mySQL database table\n
    @============================================================================================@\n
    ''', colors[randrange(2)]))

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def table_remove_options():

    print(colored('''
    @====================================== Delete Table Options ================================@\n
    1: Verwijder alle tables
    2: Verwijder geselecteerde table\n
    @============================================================================================@\n
    ''', colors[randrange(3)]))


# voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080
def mysql_error_print(e):
    print(colored("\n\tSomething went wrong: {}".format(e), "red"))
    time.sleep(2)
    sys.exit(0)

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def get_mysql_databases(user, passwd):
    """
     Je connect met de sql database via mysql_connector(), dan haal je de databases op en voegt deze onderdelen individueel toe aan de list_databases.
     Daarna wordt de list uitgeprint. Als er een exception tussen zit word de mysql_error_print opgeroepen.
     :param user:
     :param passwd:
     :return:
    """
    list_databases = []

    try:

        db, cursor = mysql_connector(user, passwd, " ")

        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

        for index in databases:
            for database in index:
                list_databases.append(database)

        print("\n\t", list_databases)

    except mysql.connector.Error as e:
        mysql_error_print(e)


# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def get_mysql_tables(user, passwd, cursor):
    """
    Je haalt je de tabellen op en voegt deze onderdelen individueel toe aan de list_tables.
    Daarna wordt de list uitgeprint. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param user:
    :param passwd:
    :param database:
    :return:
    """
    list_tables = []

    try:

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for index in tables:
            for table in index:
                list_tables.append(table)

        print("\n\t", list_tables)

    except mysql.connector.Error as e:
        mysql_error_print(e)

# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480 | izabelle Auriaux : Studentnummer: 1762808
def create_tables(cursor):
    """
    Je voegt de verschillende tabellen toe aan de database, hierin geef je aan of het een primairt key is,
    wat voor type en onder welke tabel je hem toevoegd. Dit doe je individueel voor elke tabel.
    Daarna print je dat de tabellen zijn aangemaakt. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param user:
    :param password:
    :param database:
    :return:
    """
    try:

        #cursor.execute("CREATE TABLE products (id VARCHAR(255) PRIMARY KEY UNIQUE KEY, Price INTEGER(10), Stock INTEGER(10), Flavor CHAR(255), Kleur CHAR(255), Recomendable BIT(10), Fast_mover BIT(10))")
        cursor.execute("CREATE TABLE sub_category (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, sub_category VARCHAR(255) NULL)")
        cursor.execute("CREATE TABLE main_category (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, main_category VARCHAR(255))")

        cursor.execute("CREATE TABLE doelgroep (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, doelgroep VARCHAR(255))")

        cursor.execute("CREATE TABLE gender (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, gender VARCHAR(255))")

        cursor.execute("CREATE TABLE brand (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, brand VARCHAR(255))")

        cursor.execute("CREATE TABLE profiles (id VARCHAR(255) PRIMARY KEY UNIQUE, first_order_item TIMESTAMP(6) NULL, last_order_item TIMESTAMP(6) NULL)")

        cursor.execute("CREATE TABLE sessions (id VARCHAR(255) PRIMARY KEY UNIQUE, date_from TIMESTAMP(6) NULL, date_to TIMESTAMP(6) NULL, profiles_id_key VARCHAR(255), FOREIGN KEY(profiles_id_key) REFERENCES profiles(id))")

        cursor.execute("CREATE TABLE products (id VARCHAR(255) PRIMARY KEY UNIQUE, price INTEGER(10), stock INTEGER(10), flavor VARCHAR(255) NULL, kleur VARCHAR(255) NULL, recomendable BIT(10), fast_mover BIT(10), gender_id_key INTEGER(10), doelgroep_id_key INTEGER(10), brand_id_key INTEGER(10), main_category_id_key INTEGER(10), sub_category_id_key INTEGER(10), FOREIGN KEY(gender_id_key) REFERENCES gender(id), FOREIGN KEY(brand_id_key) REFERENCES brand(id), FOREIGN KEY(main_category_id_key) REFERENCES main_category(id), FOREIGN KEY(doelgroep_id_key) REFERENCES doelgroep(id), FOREIGN KEY(sub_category_id_key) REFERENCES sub_category(id))")

        cursor.execute("CREATE TABLE `order` (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, aantal INTEGER(10) NULL, products_id_key VARCHAR(255), sessions_id_key VARCHAR(255), FOREIGN KEY(products_id_key) REFERENCES products(id), FOREIGN KEY(sessions_id_key) REFERENCES sessions(id))")

        cursor.execute("CREATE TABLE already_recommended (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, profiles_id_key VARCHAR(255), products_id_key VARCHAR(255), FOREIGN KEY(profiles_id_key) REFERENCES profiles(id), FOREIGN KEY(products_id_key) REFERENCES products(id))")


        print(colored("\n\ttables aangemaakt", "green"))

    except mysql.connector.Error as e:
        mysql_error_print(e)


# voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def delete_selected_table(table, cursor):
    """
    Je maakt een variable die een "drop table" quarry en een table bevatten.
    Vervolgens excecute je deze variable vie de cursor.
    Daarna print je dat de table is verwijderd. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param user:
    :param password:
    :param database:
    :param table:
    :return:
    """
    try:

        sql = "DROP TABLE " + table

        cursor.execute(sql)

        print(colored("\n\ttable verwijdert", "green"))

    except mysql.connector.Error as e:
        mysql_error_print(e)

# voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def delete_all_tables(database, cursor):
    """
    Je maakt een variable die een "drop database" quarry en een database inhoud
    en een variable die een "create database" quarry en een database inhoud.
    Vervolgens excecute je deze variable via de cursor.
    Daarna print je dat de tables zijn verwijdert. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param user:
    :param password:
    :param database:
    :return:
    """
    try:

        dropdatabase = "DROP DATABASE " + database
        createdatabase = "CREATE DATABASE " + database

        cursor.execute(dropdatabase)
        cursor.execute(createdatabase)

        print(colored("\n\ttables verwijdert", "green"))


    except mysql.connector.Error as e:
        mysql_error_print(e)


# voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def mysql_connector(Mysqluser, Mysqlpassword, Mysqldb):
    """
    Hier verbind je met de database aan de hand van een host, username, wachtwoord en database naam. Die aanmeldings informatie sla je op onder een
    variable. Ook maak je een cursor die de vorige variable gebruikt om aan te melden.
    Je returned de variabele met de aanmeldingsgegevens en de cursor.
    :param Mysqluser:
    :param Mysqlpassword:
    :param Mysqldb:
    :return:
    """
    mydb = mysql.connector.connect(
      host="localhost",
      user=Mysqluser,
      password=Mysqlpassword,
      database=Mysqldb
    )

    cursor = mydb.cursor()

    return mydb, cursor

# voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080
def try_except(value, value1):
    """
    Gebruik een try, except, en op basis daarvan return je value[value1] anders return je none
    :param value:
    :param value1:
    :return return value[value1]
    :except return "None"
    """
    try:
        return value[value1]
    except:
        return "None"

# voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080
def try_except_longer(value, value1, value2):
    """
    Gebruik een try, except, en op basis daarvan return je value[value1][value2] anders return je none
    :param value:
    :param value1:
    :param value2:
    :return return value[value1][value2]
    :except return "None"
    """
    try:
        return value[value1][value2]
    except:
        return "None"


# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480
def get_item_from_collection(mongodb, Mysqldb, Mysqluser, Mysqlpassword, db, cursor):
    """
    Maak een lege list voor de verschillende categorieen producten, merken, gender, doelgroep, sub cat & sub sub cat.
    Maak hiervoor ook lists die een profiel gaan voorstellen. En lege lists die collecties voorstellen.
    Daarna connect je met de mongo database en haal je de data op uit de database, deze verdeel je via de lege list waar het toebehoorend is.
    Als dat geslaagd is, print een melding
    uiteindelijk geef je het door naar insert_into_mysql.
    :param Mysqldb:
    :param Mysqluser:
    :param Mysqlpassword:
    :return:
    """
    #opslaan van de values van collection products
    products_value_item = []

    brand_value_item = []
    gender_value_item = []
    doelgroep_value_item = []
    sub_category_value_item = []
    sub_sub_category_value_item = []

    #opslaan van de values van collection profiles

    profiles_values = []
    profiles_value_item_buid = []
    profiles_value_item_buid_copy = []
    profiles_previously_recommended = []

    #opslaan van de values van collection sessions

    sessions_date_to_from = []
    sessions_buid = []
    sessions_orders = []

    #connectie naar het mongodb database
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[mongodb]

    col_products = mydb["products"]
    col_session = mydb["sessions"]
    col_profile = mydb["profiles"]


    # for loop voor het ophalen van data vanuit MongoDB
    for value in col_products.find({}, {"_id", "price", "properties", "flavor", "properties", "recommendable", "fast_mover", "brand", "gender", "properties", "sub_category", "sub_sub_category"}):
        column_id = try_except(value, "_id")
        column_price = try_except_longer(value, "price", "selling_price")
        column_stock = try_except_longer(value, "properties", "stock")
        column_flavor = try_except(value, "flavor")
        column_kleur = try_except_longer(value, "properties", "kleur")
        column_recommendable = try_except(value, "recommendable")
        column_fast_mover =  try_except(value, "fast_mover")
        products_value_item.append([column_id, column_price, column_stock, column_flavor, column_kleur, column_recommendable, column_fast_mover])

        # data uithalen voor brand, gender, doelgroep, main_category, sub_category
        column_brand = try_except(value, "brand")
        brand_value_item.append(column_brand)

        column_gender = try_except(value, "gender")
        gender_value_item.append(column_gender)

        column_gender_doelgroep = try_except_longer(value, "properties", "doelgroep")
        doelgroep_value_item.append(column_gender_doelgroep)

        sub_category = try_except(value, "sub_category")
        sub_category_value_item.append(sub_category)

        sub_sub_category = try_except(value, "sub_sub_category")
        sub_sub_category_value_item.append(sub_sub_category)

    for value in col_profile.find({}, {"_id", "buids", "previously_recommended", "order"}):

        column_id = try_except(value, "_id")
        column_first = try_except_longer(value, "order", "first")
        column_latest = try_except_longer(value, "order", "latest")
        profiles_values.append([str(column_id), str(column_first), str(column_latest)])

        column_buid = try_except(value, "buids")
        profiles_value_item_buid.append(str(column_buid))

    dict = {}
    list_test = []
    for value in col_session.find({}, {"buid", "session_start", "session_end", "order"}):


        orders = try_except_longer(value, "order", "products")
        if orders != "None" and orders != None:
            for key in orders:
                list_test.append(key["id"])


        column_start = try_except(value, "session_start")
        column_end = try_except(value, "session_end")

        column_buid = try_except(value, "buid")
        if column_buid == None:
            column_buid = ["None"]

        sessions_date_to_from.append([' '.join([str(item) for item in column_buid ]), str(column_start), str(column_end)])

    count = 0
    for item in list_test:
        for item_dub in list_test:
            if item == item_dub:
                count += 1
        dict[item] = count

    insert_into_mysql(Mysqluser, Mysqlpassword, Mysqldb, db, cursor, products_value_item, brand_value_item, gender_value_item, doelgroep_value_item, sub_category_value_item, sub_sub_category_value_item, profiles_previously_recommended, profiles_value_item_buid, profiles_values, sessions_date_to_from)

# voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def setting_list(list_value):
    """
    Maakt van list_value een set list, en retured deze.
    :param list_value:
    :return:
    """
    list_category_value = list(set(list_value))
    return list_category_value

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503
def loop_through_value(list_value, db, cursor, table, column):
    """
    Connect aan de database en loop door de verschillende waardes in de list, voeg ze dan toe aan de table. Je voegt hier maar 1 waarde toe aan de table en collum.
    Execute deze command en commit het naar de sql database.
    :param list_value:
    :param db:
    :param table:
    :param column:
    :return:
    """
    mycursor = db.cursor()
    for item in list_value:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column + ") VALUES (%s)"
        category_list_sql_value = (item, )
        mycursor.execute(category_list_sql, category_list_sql_value)

    db.commit()

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503
def loop_through_three_values(list_value, db, cursor, table, column, column1, column2):
    """
    Connect aan de database en loop door de verschillende waardes in de list, voeg ze dan toe aan de table. Je voegt hier 3 waardes toe aan de table en collum.
    Execute deze command en commit het naar de sql database.
    :param list_value:
    :param db:
    :param cursor:
    :param table:
    :param column:
    :param column1:
    :param column2:
    :return:
    """
    mycursor = db.cursor()
    for item in list_value:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column + ", " + column1 + ", " + column2 + ") VALUES (%s, %s, %s)"
        category_list_sql_value = (item[0], item[1], item[2])
        mycursor.execute(category_list_sql, category_list_sql_value)

    db.commit()

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503
def loop_through_values(list_value, db, cursor, table, column, column1, column2, column3, column4, column5, column6):
    """
    Connect aan de database en loop door de verschillende waardes in de list, voeg ze dan toe aan de table. Je voegt hier 6 waardes toe aan de table en collum.
    Execute deze command en commit het naar de sql database.
    :param list_value:
    :param db:
    :param cursor:
    :param table:
    :param column:
    :param column1:
    :param column2:
    :param column3:
    :param column4:
    :param column5:
    :param column6:
    :return:
    """
    mycursor = db.cursor()

    for list1 in list_value:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column + ", " + column1 + ", " + column2 + ", " + column3 + ", " + column4 + ", " + column5 + ", " + column6 + ") VALUES (%s, %s, %s, %s, %s, %s, %s)"
        category_list_sql_value = (list1[0], list1[1], list1[2], list1[3], list1[4], list1[5], list1[6])
        mycursor.execute(category_list_sql, category_list_sql_value)

    db.commit()

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503
def insert_into_mysql(Mysqluser, Mysqlpassword, Mysqldb, db, cursor, products_list, brand_list, gender_list, doelgroep_list, sub_category_list, sub_sub_category_list, profiles_previously_recommended, profiles_value_item_buid, profiles_values, sessions_date_to_from):
    """
    Connect aan de database, gebruik de loop_through_value() om de verschillende waarde aan de bijbehoorende table toe te voegen.
    Start de tijd aan het begin van dit proces en stop het wanneer het klaar is. Uiteindelijk word er geprint dat dit is gelukt. Ook de tijd
    wordt getoond.
    :param Mysqluser:
    :param Mysqlpassword:
    :param Mysqldb:
    :param products_list:
    :param brand_list:
    :param gender_list:
    :param doelgroep_list:
    :param sub_category_list:
    :param sub_sub_category_list:
    :return:
    """
    print(colored("\n\tMysql tables word gevuld", "yellow"))

    start = time.time()

    #hier initialiseren we mysql connection

    #hier word de data geinsert
    set_brand_list = setting_list(brand_list)
    set_gender_list = setting_list(gender_list)
    set_doelgroep_list = setting_list(doelgroep_list)
    set_sub_category_list = setting_list(sub_category_list)
    set_sub_sub_category_list = setting_list(sub_sub_category_list)


    loop_through_value(set_brand_list, db, cursor, "brand", "brand")
    loop_through_value(set_gender_list, db, cursor, "gender", "gender")
    loop_through_value(set_doelgroep_list, db, cursor, "doelgroep", "doelgroep")
    loop_through_value(set_sub_category_list, db, cursor, "main_category", "main_category")
    loop_through_value(set_sub_sub_category_list, db, cursor, "sub_category", "sub_category")


    loop_through_values(products_list, db, cursor, "products", "id", "price", "stock", "flavor", "kleur", "recomendable", "fast_mover")
    print("products done")

    loop_through_three_values(sessions_date_to_from, db, cursor, "sessions", "id", "date_from", "date_to")
    print("sessions done")

    loop_through_three_values(profiles_values, db, cursor, "profiles", "id", "first_order_item", "last_order_item")
    print("profiles done")

    end = time.time()
    print(colored("\n\tMysql tables is gevuld", "green"))
    print(colored("\n\tTotal time taken, " + str(round(end - start, 6)) + " seconds", "green"))

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503 en Ceyhun Cakir, Studentnummer: 1784480
def from_mongo_to_mysql():

    """
    Connect met mongo en sql databases. Als de mongo database bestaat worden collecties toegevoegd in een lijst waardoor de gebruiker kan zien welke collections er zijn.
    Gebruiker geeft username + wachtwoord op van mysql database
    mysql_connector word opgeroepen om de user aantemelden aan de mysql session
    tables worden verwijderd en aangemaakt
    data word overgezet van mongodb naar mysql
    :return:
    """

    mysql_rules()

    #connectie naar het mongodb database
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    dbnames = client.list_database_names()

    #lijst voor het opslaan van de collections

    while True:

        print("\n\t", dbnames)
        database = input(colored("\n\tGeef je MongoDB database op: > ", "yellow"))
        collection = client[database]

        # controleert of de database bestaat in mongodb
        if database in dbnames:

            # het toevoegen van collections van de geselecteerde database
            user = input(colored("\n\tGeef je mySQL username op: > ", "yellow"))
            passwd = input(colored("\n\tGeef je mySQL password op: > ", "yellow"))

            get_mysql_databases(user, passwd)

            my_sql_database = input(colored("\n\tGeef je Mysql database op: > ", "yellow"))

            db, cursor = mysql_connector(user, passwd, my_sql_database)
            #verwijdert geselecteerde database
            delete_all_tables(my_sql_database, cursor)

            cursor.execute("USE " + my_sql_database)

            # maak aan geselecteerde database met tables
            create_tables(cursor)

            # process voor het geselecteerde input "products"
            get_item_from_collection(database, my_sql_database, user, passwd, db, cursor)

            cursor.close()
            db.close()
            sys.exit(0)


        else:
            print(colored("\n\t# Database " + database + " bestaat niet!", "red"))

# voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080
def delete_mysql_table_process(user, passwd, database, cursor):

    """
    Vraag om een optie, als optie 1 word gebruikt roep je delete_all_tables() op.
    Als optie 2 word gebruikt roep je get_mysql_tables() en geef hier een table op die je wilt verwijderen. Vervolgens word via
    delete_selected_table() automatisch deze table verwijderd.
    Als er een andere optie word gegeven komt er een foutmelding.
    :return:
    """

    table_remove_options()

    optie = input(colored("\n\tSelecteer je optie: > ", "yellow"))

    if optie == "1":

            #verwijdert de hele geselecteerde database en creert het opnieuw aan
        delete_all_tables(database, cursor)


    elif optie == "2":

            #pakt de geselecteerde database tables
        get_mysql_tables(user, passwd, cursor)

        table = input(colored("\n\tGeef je mySQL table op: > ", "yellow"))

        delete_selected_table(table, cursor)

    else:
        print(colored("\n\tdie optie bestaat niet", "red"))


# voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080
def make_mysql_table_process(user, passwd, cursor):

    """
    Roep mysql_rules() op en create_tables(x) met een gegeven cursor variable op de x.

    :return:
    """

    mysql_rules()

    create_tables(cursor)


# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480, Kenny van den Berg, Studentnummer: 1777503
def menu():

    """
    Roep de banner en het optie menu op. Als optie input 1 is vraag je om een user, password & database input, roep daarna mysql_connector() op om je aan te melden.
    make_mysql_table_process word opgeroepen uiteindelijk na het aanmelding
    Als optie input 2 is roep je from_mongo_to_mysql() op.
    Als optie input 3 is vraag je om een user & password input, en gebruik get_mysql_databse() om mysql databases te zien.
    gebruik daarna een database input en roep mysql_connector() op om je aan te melden.
    Roep daarna delete_mysql_table_process() op om het process af te ronden.
    Als het anders is dan het normale user input formaat geef je een foutmelding en sluit je de interface af
    :return:
    """

    banner()
    opties()

    optie = input(colored("\n\tKies je optie: > ", "yellow"))


    if optie == "1":

        user = input(colored("\n\tGeef je mySQL username op: > ", "yellow"))
        passwd = input(colored("\n\tGeef je mySQL password op: > ", "yellow"))
        database = input(colored("\n\tGeef je mySQL database op: > ", "yellow"))
        db, cursor = mysql_connector(user, passwd, database)

        #process voor het aanmaken van mysql tables
        make_mysql_table_process(user, passwd, cursor)

        cursor.close()
        db.close()
        sys.exit(0)
    elif optie == "2":
        from_mongo_to_mysql()
    elif optie == "3":

        user = input(colored("\n\tGeef je mySQL username op: > ", "yellow"))
        passwd = input(colored("\n\tGeef je mySQL password op: > ", "yellow"))

        get_mysql_databases(user, passwd)

        database = input(colored("\n\tGeef je mySQL database op: > ", "yellow"))
        db, cursor = mysql_connector(user, passwd, database)
        #process voor het verwijderen van mysql tables
        delete_mysql_table_process(user, passwd, database, cursor)

        cursor.close()
        db.close()
        sys.exit(0)
    else:
        print(colored("\n\tdie optie bestaat niet", "red"))
        time.sleep(1)
        sys.exit(0)


menu()

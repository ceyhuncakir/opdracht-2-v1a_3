import pymongo
from random import *
import sys
import mysql.connector
import time

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503 en Ceyhun Cakir, Studentnummer: 1784480
def mongo_database_names():
    """Set up connection with local mongodb and retrieve a list of all databases names
    :param mongo_client, :param db_names
    """
    # Set connection with mongo db
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Retrieve the names of all databases
    db_names = mongo_client.list_database_names()

    # Close connection
    mongo_client.close()
    return db_names

# Voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def mysql_connector(user, password, db_name):
    """
    Hier verbind je met de database aan de hand van een host, username, wachtwoord en database naam. Die aanmeldings informatie sla je op onder een
    variable. Ook maak je een cursor die de vorige variable gebruikt om aan te melden.
    Je returned de variabele met de aanmeldingsgegevens en de cursor.
    :param user:, :param password:, :param db_name:
    """
    # Variable with a connection with mysql with the user, password and db_name
    db = mysql.connector.connect(host="localhost", user=user, password=password, database=db_name)

    # Variable for mysql cursor
    cursor = db.cursor()
    return db, cursor


# Voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808 en Wytze A. Ketel, Studentnummer: 1797080
def sql_closer(db, cursor):
    """
    Sluit de cursor en commit de veranderingen van de database daarna sluit de database.
    :param cursor:, :param db:
    """
    # Close the cursor, commit de change made in the database and close the database
    cursor.close()
    db.commit()
    db.close()


# Voornamelijk gemaakt door: Wytze A. Ketel, Studentnummer: 1797080 en Ceyhun Cakir, Studentnummer: 1784480
def get_mysql_database_names(user, passwd):
    """
    Maak connectie met de data base en execute een query voor het opvragen van de database namen. Loop door de lijsten heen
    heen en append elke item in een lijst en return de lijst.
    :param user:
    :param passwd:
    :return:
    """
    # list variable for the databases
    db, cursor = mysql_connector(user, passwd, " ")
    database_name_list = []

    # Execute sql query to retrieve all database names
    cursor.execute("SHOW DATABASES")

    # varible with all items found
    database_names = cursor.fetchall()
    sql_closer(db, cursor)

    # retrieve the database names from with in a list with in a list
    for item in database_names:
        for db_name in item:
            database_name_list.append(db_name)
    return database_name_list


# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480 en Wytze A. Ketel, Studentnummer: 1797080
def retrieve_mysql_table_names(cursor):
    """"
    Execute een query voor het opvragen van alle tabelen in de database. Return een lijst aan table namen.
    :param cursor:
    """
    # list for all tables
    list_tables = []

    # retrieve all tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    # append each table to list_tables
    for index in tables:
        for table in index:
            list_tables.append(table)
    return list_tables

# voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808 en Wytze A. Ketel, Studentnummer: 1797080
def delete_database(database_name, cursor):
    """
    Je maakt een variable die een "drop database" quarry en een database inhoud
    en een variable die een "create database" quarry en een database inhoud.
    Vervolgens excecute je deze variable via de cursor.
    Daarna print je dat de tables zijn verwijdert. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param cursor:
    :param database_name:
    :return:
    """
    # 2 varibles with my sql queries
    drop_query = "DROP DATABASE " + database_name
    create_query = "CREATE DATABASE " + database_name

    # Execute sql query drop and create
    cursor.execute(drop_query)
    cursor.execute(create_query)

# Voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480 | izabelle Auriaux : Studentnummer: 1762808
def create_predefined_tables(cursor, database_name):
    """ predefined
    Je voegt de verschillende tabellen toe aan de database, hierin geef je aan of het een primairt key is,
    wat voor type en onder welke tabel je hem toevoegd. Dit doe je individueel voor elke tabel.
    Daarna print je dat de tabellen zijn aangemaakt. Als er een exception tussen zit word de mysql_error_print opgeroepen.
    :param user:, :param password:, :param database:, :return:
    """
    cursor.execute("USE " + database_name)
    cursor.execute("CREATE TABLE sub_category (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, sub_category VARCHAR(255) NULL)")
    cursor.execute("CREATE TABLE main_category (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, main_category VARCHAR(255))")

    cursor.execute("CREATE TABLE doelgroep (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, doelgroep VARCHAR(255))")

    cursor.execute("CREATE TABLE gender (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, gender VARCHAR(255))")

    cursor.execute("CREATE TABLE brand (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, brand VARCHAR(255))")

    cursor.execute("CREATE TABLE profiles (id VARCHAR(255) PRIMARY KEY UNIQUE, first_order_item TIMESTAMP(6) NULL, last_order_item TIMESTAMP(6) NULL)")

    cursor.execute("CREATE TABLE sessions (id VARCHAR(255) PRIMARY KEY UNIQUE, date_from TIMESTAMP(6) NULL, date_to TIMESTAMP(6) NULL, profiles_id_key VARCHAR(255), FOREIGN KEY(profiles_id_key) REFERENCES profiles(id))")

    cursor.execute("CREATE TABLE products (id VARCHAR(255) PRIMARY KEY UNIQUE, price INTEGER(10), stock INTEGER(10), flavor VARCHAR(255) NULL, kleur VARCHAR(255) NULL, recomendable BIT(10), fast_mover BIT(10), gender_id_key INTEGER(10), doelgroep_id_key INTEGER(10), brand_id_key INTEGER(10), main_category_id_key INTEGER(10), sub_category_id_key INTEGER(10), FOREIGN KEY(gender_id_key) REFERENCES gender(id), FOREIGN KEY(brand_id_key) REFERENCES brand(id), FOREIGN KEY(main_category_id_key) REFERENCES main_category(id), FOREIGN KEY(doelgroep_id_key) REFERENCES doelgroep(id), FOREIGN KEY(sub_category_id_key) REFERENCES sub_category(id))")

    cursor.execute("CREATE TABLE orders (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, aantal INTEGER(10) NULL, products_id_key VARCHAR(255), sessions_id_key VARCHAR(255), FOREIGN KEY(products_id_key) REFERENCES products(id), FOREIGN KEY(sessions_id_key) REFERENCES sessions(id))")

    cursor.execute("CREATE TABLE already_recommended (id INTEGER(10) AUTO_INCREMENT PRIMARY KEY UNIQUE, profiles_id_key VARCHAR(255), products_id_key VARCHAR(255), FOREIGN KEY(profiles_id_key) REFERENCES profiles(id), FOREIGN KEY(products_id_key) REFERENCES products(id))")


#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503 en Ceyhun Cakir, Studentnummer: 1784480
def try_except_flex(*value):
    """
    #     Gebruik een try except over de gegeven dict values, return  de dict value anders return je none
    #     :param value:, :param value1:, :return value[value1], :except return "None"
    #
    """
    # Een extra variable mee geven voor de keuze argument[0]
    try:
        if value[0] == 0:
            return value[1][value[2]]
        else:
            return value[1][value[2]][value[3]]
    except:
        return "None"

# Voornamelijk gemaakt door: izabelle Auriaux, Studentnummer: 1762808
def setting_list(list_value):
    """
    Maakt van list_value een set list, en retured deze.
    :param list_value:
    :return:
    """
    list_category_value = list(set(list_value))
    return list_category_value


# voornamelijk gemaakt door: Ceyhun Cakir, Studentnummer: 1784480 en Kenny van den berg Studentnummer: 1777503
def get_item_from_collection(db, cursor, database):
    """
    Maak een lege list voor de verschillende categorieen producten, merken, gender, doelgroep, sub cat & sub sub cat.
    Maak hiervoor ook lists die een profiel gaan voorstellen. En lege lists die collecties voorstellen.
    Daarna connect je met de mongo database en haal je de data op uit de database, deze verdeel je via de lege list waar het toebehoorend is.
    uiteindelijk geef je het door naar insert_into_mysql.
    :param db:
    :param cursor:
    :param database:
    :return:
    """
    # Opslaan van de values van collection products
    products_value_item = []

    # Categories of products
    product_categorie_list = [[], [], [], [], []]

    # Opslaan van de values van collection profiles
    profiles_values = []
    profiles_previously_recommended = []
    profiles_sessions_list = {}

    #opslaan van de values van collection sessions
    sessions_date_to_from = []
    sessions_orders = []

    #connectie naar het mongodb database
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Retrieve a list of all collections
    collections = mongo_client[database]

    # retrieve collections products, sessions and profiles
    collection_products = collections["products"]

    collection_session = collections["sessions"]

    collection_profile = collections["profiles"]

    # for loop voor het ophalen van data vanuit MongoDB
    for item in collection_products.find({}, {"_id", "price", "properties", "flavor", "properties", "recommendable", "fast_mover", "brand", "gender", "properties", "sub_category", "sub_sub_category"}):
        product_value_one = []
        product_value_one.append(try_except_flex(0, item, "_id"))                            # Id = index 0
        product_value_one.append(try_except_flex(1, item, "price", "selling_price"))         # Price = index 1
        product_value_one.append(try_except_flex(1, item, "properties", "stock"))            # Stock = index 2
        product_value_one.append(try_except_flex(0, item, "flavor"))                         # Flavor = index 3
        product_value_one.append(try_except_flex(1, item, "properties", "kleur"))
        product_value_one.append(try_except_flex(0, item, "recommendable"))
        product_value_one.append(try_except_flex(0, item, "fast_mover"))
        product_value_one.append(try_except_flex(0, item, "gender"))
        product_value_one.append(try_except_flex(1, item, "properties", "doelgroep"))
        product_value_one.append(try_except_flex(0, item, "brand"))
        product_value_one.append(try_except_flex(0, item, "sub_category"))
        product_value_one.append(try_except_flex(0, item, "sub_sub_category"))

        products_value_item.append(product_value_one)

        # data uithalen voor brand, gender, doelgroep, main_category, sub_category
        product_categorie_list[0].append(try_except_flex(0, item, "brand"))                        # brand = index 0
        product_categorie_list[1].append(try_except_flex(0, item, "gender"))                       # gender = index 1
        product_categorie_list[2].append(try_except_flex(1, item, "properties", "doelgroep"))      # properties = index 2
        product_categorie_list[3].append(try_except_flex(0, item, "sub_category"))                 # sub_category = index 3
        product_categorie_list[4].append(try_except_flex(0, item, "sub_sub_category"))             # sub_sub_category = index 4


    for item in collection_profile.find({}, {"_id", "buids", "previously_recommended", "order"}):
        column_id = try_except_flex(0,item, "_id")
        profiles_values.append([str(try_except_flex(0, item, "_id")), str(try_except_flex(1,item, "order", "first")),
                                str(try_except_flex(1, item, "order", "latest"))])

        column_buid = try_except_flex(0, item, "buids")

        # if there are multiple build loop through the list and add each buid and profile id to a dictionary
        if type(column_buid) == list:
            for index_item in column_buid:
                profiles_sessions_list[index_item] = str(column_id)


    for item in collection_session.find({}, {"buid", "session_start", "session_end", "order"}):
        column_start = try_except_flex(0, item, "session_start")
        column_end = try_except_flex(0, item, "session_end")
        column_buid = try_except_flex(0, item, "buid")

        id = try_except_flex(0, item, "_id")
        order = try_except_flex(0, item, "order")

        item_dict = {}
        item_list = []

        # if order is a dictionary
        if type(order) == dict:

            # for each product in the order
            for item in order['products']:
                # if the item is not in the dict, create item key and value 1
                if item['id'] not in item_dict:
                    item_dict[item['id']] = 1
                # else increment the value with one
                else:
                    item_dict[item['id']] += 1

            # for each key, value in item dict append the value, key and id.
            for key, value in item_dict.items():
                item_list.append([value, key, str(id)])

        # if the length of the list is not 0 append item
        if len(item_list) != 0:
            for item in item_list:
                sessions_orders.append(item)

        # Retrieve correct id build of profile to sessions.
        try:
            id_buid = profiles_sessions_list[column_buid[0]]
        except (TypeError, KeyError):
            id_buid = "None"

        sessions_date_to_from.append([str(id), str(column_start), str(column_end), id_buid])
    mongo_client.close()

    end, start = insert_into_mysql_process(db, cursor, products_value_item, product_categorie_list, profiles_previously_recommended, profiles_values, sessions_date_to_from, sessions_orders)
    return end, start


#  Voornamelijk gemaakt door Ceyhun Cakir, Studentnummer: 1784480 en Kenny van den berg Studentnummer: 1777503
def retrieve_tabel_data(table, db):
    """
    Maak een dictionary aan en execute een querry voor het opvragen van alle data in het table.
    voor elke item in de lijst voeg de key en value toe aan een dictionary
    :param table:
    :param db:
    :return:
    """
    # divine a dictonary
    result_dict = {}
    cursor = db.cursor()

    # retrieve all items from selected table
    cursor.execute("SELECT * FROM " + table)
    myresult = cursor.fetchall()

    # for each item in myresult
    for item in myresult:
        result_dict[item[1]] = item[0]
    return result_dict

# Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503
def edit_key_data(products_list, db):
    """
    Roep voor iedere category de functie retrieve_tabel_data op. loop door de geven lijst en vergelijk of het product
    item hetzelfde als als de dictionary item als dat het geval is verander het product item naar de dictonary key.
    :param products_list:
    :param db:
    :return:
    """
    # retrieve all category table's and the rows
    gender_dict = retrieve_tabel_data('gender', db)
    doelgroep_dict = retrieve_tabel_data('doelgroep', db)
    brand_dict = retrieve_tabel_data('brand', db)
    main_category_dict = retrieve_tabel_data('main_category', db)
    sub_category = retrieve_tabel_data('sub_category', db)

    # loop trough the list and change the name of category to the table key
    for item in products_list:
        item[7] = gender_dict[item[7]]
        item[8] = doelgroep_dict[item[8]]
        item[9] = brand_dict[item[9]]
        item[10] = main_category_dict[item[10]]
        item[11] = sub_category[item[11]]
    # print(products_list)
    return products_list

#  Voornamelijk gemaakt door Kenny van den berg Studentnummer: 1777503 en Ceyhun Cakir, Studentnummer: 1784480
def insert_values(direction, list_value, db, cursor, table, *column):
    """
    Connect aan de database en loop door de verschillende waardes in de list en voeg ze die dan toe aan de table collums.
    Execute deze command en commit het naar de sql database.
    :param direction:, :param list_value:, :param db:, :param cursor:, :param table:, :param *column:, :return:,
    """
    # Switch between diffrent direction for table insert
    if direction == 0:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column[0] + ") VALUES (%s)"
        for item in list_value:
            category_list_sql_value = (item, )
            cursor.execute(category_list_sql, category_list_sql_value)

    elif direction == 1:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column[0] + ", " + column[1] + ", " + column[2] + ") VALUES (%s, %s, %s)"
        for item in list_value:
            category_list_sql_value = (item[0], item[1], item[2])
            cursor.execute(category_list_sql, category_list_sql_value)

    elif direction == 2:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column[0] + ", " + column[1] + ", " + column[2] + ", " + column[3] + ", " + column[4] + ", " + column[5] + ", " + column[6] + ", " + column[7] + ", " + column[8] + ", " + column[9] + ", " + column[10] + ", " + column[11] + ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for list1 in list_value:
            category_list_sql_value = (list1[0], list1[1], list1[2], list1[3], list1[4], list1[5], list1[6], list1[7], list1[8], list1[9], list1[10], (list1[11]))
            cursor.execute(category_list_sql, category_list_sql_value)

    elif direction == 3:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column[0] + ", " + column[1] + ", " + column[2] + ", " + column[3] + ") VALUES (%s, %s, %s, %s)"
        for item in list_value:
            category_list_sql_value = (item[0], item[1], item[2], item[3])
            cursor.execute(category_list_sql, category_list_sql_value)

    elif direction == 4:
        category_list_sql = "INSERT IGNORE INTO " + table + " (" + column[0] + ", " + column[1] + ", " + column[2] + ") VALUES (%s, %s, %s)"
        for item in list_value:
            category_list_sql_value = (item[0], item[1], item[2])
            cursor.execute(category_list_sql, category_list_sql_value)
    db.commit()


# #  Voornamelijk gemaakt door Ceyhun Cakir, Studentnummer: 1784480 en Kenny van den berg Studentnummer: 1777503
def insert_into_mysql_process(db, cursor, products_list, product_categorie_list, profiles_previously_recommended, profiles_values, sessions_date_to_from, sessions_orders):
    """
    Connect aan de database, gebruik de loop_through_value() om de verschillende waarde aan de bijbehoorende table toe te voegen.
    Start de tijd aan het begin van dit proces en stop het wanneer het klaar is. Uiteindelijk word er geprint dat dit is gelukt. Ook de tijd
    wordt getoond.
    :param db:, :param cursor:, :param products_list:, :param product_categorie_list:, :param profiles_previously_recommended:
    :param profiles_values:, :param sessions_date_to_from:, :param sessions_orders:
    """

    start = time.time()

    # duplicates of the same category are filtered out to retrieve a list with one of each category item,
    set_brand_list = setting_list(product_categorie_list[0])
    set_gender_list = setting_list(product_categorie_list[1])
    set_doelgroep_list = setting_list(product_categorie_list[2])
    set_sub_category_list = setting_list(product_categorie_list[3])
    set_sub_sub_category_list = setting_list(product_categorie_list[4])

    # Insert values into the brand tabels.
    insert_values(0, set_brand_list, db, cursor, "brand", "brand")
    insert_values(0, set_gender_list, db, cursor, "gender", "gender")
    insert_values(0, set_doelgroep_list, db, cursor, "doelgroep", "doelgroep")
    insert_values(0, set_sub_category_list, db, cursor, "main_category", "main_category")
    insert_values(0, set_sub_sub_category_list, db, cursor, "sub_category", "sub_category")

    # Edit products based on there categorty
    products_list = edit_key_data(products_list, db)

    # Insert products, profiles, sessions and orders into MySql database.
    insert_values(2, products_list, db, cursor, "products", "id", "price", "stock", "flavor", "kleur", "recomendable", "fast_mover", "gender_id_key", "doelgroep_id_key", "brand_id_key", "main_category_id_key", "sub_category_id_key")

    insert_values(1, profiles_values, db, cursor, "profiles", "id", "first_order_item", "last_order_item")

    insert_values(3, sessions_date_to_from, db, cursor, "sessions", "id", "date_from", "date_to", "profiles_id_key")

    insert_values(4, sessions_orders, db, cursor, "orders", "aantal", "products_id_key", "sessions_id_key")

    end = time.time()
    return end, start

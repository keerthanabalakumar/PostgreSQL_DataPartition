#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import sys
temp=0;
rrpartition=0;
rpartition=0;
step1=0;

DATABASE_NAME = 'dds_assgn1'



def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)

    cur.execute(
        "CREATE TABLE " + ratingstablename + " (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp2 VARCHAR(10),  Rating REAL, temp3 VARCHAR(10), Timestamp INT)")

    loadout = open(ratingsfilepath, 'r')

    cur.copy_from(loadout, ratingstablename, sep=':',
                  columns=('UserID', 'temp1', 'MovieID', 'temp2', 'Rating', 'temp3', 'Timestamp'))
    cur.execute(
        "ALTER TABLE " + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp2,DROP COLUMN temp3, DROP COLUMN Timestamp")

    cur.close()
    openconnection.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    name = "range_part"
    global rpartition
    rpartition = numberofpartitions
    global step1
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % ratingstablename)
        Mini= 0.0
        Maxi = 5.0
        incrementvalue = (Maxi - Mini) / (float)(numberofpartitions)
        step1=incrementvalue;
        i = 0;

        while i < numberofpartitions:
            newTableName = name + `i`
            cursor.execute("DROP TABLE IF EXISTS " + newTableName)
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (newTableName))
            i += 1;

        i = 0;
        while Mini < Maxi:
            limit = Mini
            ulimit = Mini + incrementvalue
            if limit == 0.0:
                cursor.execute(
                  "SELECT * FROM %s WHERE Rating >= %f AND Rating <= %f" % (ratingstablename, limit, ulimit))
            else:
                 cursor.execute(
                    "SELECT * FROM %s WHERE Rating > %f AND Rating <= %f" % (ratingstablename, limit, ulimit))
            rows = cursor.fetchall()
            newTableName = name + `i`
            for row in rows:
                cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
                newTableName, row[0], row[1], row[2]))
            Mini = ulimit
            i += 1;
        openconnection.commit()
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except psycopg2.DatabaseError, e:
        if openconnection:
           openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
           cursor.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    name = "rrobin_part"
    global rrpartition
    rrpartition = numberofpartitions
    try:
        cursor = openconnection.cursor()
        x = 0
        ulimit = numberofpartitions
        cursor.execute("SELECT * FROM %s" % ratingstablename)
        rows = cursor.fetchall()
        lastelement = 0
        for x in range(0,ulimit,1):
            newTableName = name + `x`
            cursor.execute("DROP TABLE IF EXISTS " + newTableName)
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (newTableName))
            x += 1
            #print newTableName
            lastelement = (lastelement + 1)
            y = (lastelement % numberofpartitions)

        for row in rows:
                newTableName = name + `y`
                cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
                newTableName, row[0], row[1], row[2]))
                lastelement = (lastelement + 1)% numberofpartitions
                y = lastelement
        global temp
        temp = lastelement
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except psycopg2.DataError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
           cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    name = "rrobin_part"
    try:
        cursor = openconnection.cursor()
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
            ratingstablename, userid, itemid, rating))
        global rrpartition
        global temp
        newTableName = name + `temp`
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
            newTableName, userid, itemid, rating))
        y = ((temp+1) % rrpartition)
        temp =y;
        openconnection.commit()
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        if cursor:
           cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    name = "range_part"
    startvalue =0.0
    maxi=5.0
    global step1
    global rpartition
    j=step1
    i=0
    count1=0

    while(startvalue < maxi):
         if(startvalue <= rating):
             if(rating <=j):
                count=i
                break
         startvalue = startvalue+step1
         j= step1+j
         i=i+1
    try:
        cursor = openconnection.cursor()
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
            ratingstablename, userid, itemid, rating))
        newTableName = name + `count`
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
            newTableName, userid, itemid, rating))
        openconnection.commit()
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
           cursor.close()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cursor.fetchall()
        for t_name in tables:
            cursor.execute('DROP TABLE %s CASCADE' % (t_name[0]))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()




# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings', 'test_data.dat', con)
            rangepartition('ratings', 5, con)

            roundrobinpartition('ratings', 3, con)
            roundrobininsert('ratings', 2, 123, 4.5, con)
            roundrobininsert('ratings', 0, 00, 0.5, con)
            rangeinsert('ratings',0,0000,4.5,con)
            deleteTables('ratings',con)

            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
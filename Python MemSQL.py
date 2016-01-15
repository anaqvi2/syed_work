import MySQLdb
import datetime
import thread
import time

def write(): # Write is the name of the function
    try: # Best practice
    
        db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2') # Connect to MemSQL DATABASE

        cur = db.cursor() # Cursor to point to table

        startime = datetime.datetime.now() # Start the time

        i=1 # Simple iterator initiated at 1
        while i<=1000000: # While loop will run 10 times until i is equal to 1000000
            cur.execute('''
                INSERT INTO IBI_UNION (SCAN_ID, SCAN_SHA, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_NBR, IOS_LOAD_DATE, MPT_LOAD_DATE, LOAD_DATE)
                VALUES ("%i", "text1", "tx2", 1111, "text3", "text4", NOW(), NOW(), NOW())''' % (i)) # Adds sample data into table, making sure Primary Key is Unique
            i=i+1 # I is iterated over until it reaches the end of the lopp

        endtime = datetime.datetime.now() # Stop the timer
        #print "Time To Insert 10 Rows is", endtime - startime # Time taken to perform 1000000 Inserts
        Total_sec = (endtime-startime).total_seconds() # Total time take to perform 1000000 Inserts in seconds
        Single_Row = 1000000/Total_sec # Time for a single Insert Query
        print "INSERT per second for Writing a Single Row Is:", Single_Row 
        db.commit()
        db.close()
    
    except Exception, e:
        print str(e)

def read(): # Read is the name of the function

    try:
        db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2')

        cur = db.cursor()

        startime = datetime.datetime.now()

        db.query('SELECT * FROM IBI_UNION') # Select Query, Selects all 1000000 values from the Previous Write Query from The IBI_UNION table in the database

        result = db.use_result() # Storing the result

        row = result.fetch_row() # Storing the row
        while row:
            #print row # Previously Printed Row by Row
            row = result.fetch_row() # Retrieves row data

        endtime = datetime.datetime.now()
        #print "Time To Read 1000000 Rows is", endtime - startime
        Total_sec = (endtime-startime).total_seconds()
        Single_Row = 1000000/Total_sec
        print "SELECT per second for Reading a Single Row Is:", Single_Row
        db.commit()
        db.close()

    except Exception, e:
        print str(e)

def mix(): # Mix is the name of the function, Find INSERT, AND SELECT THROUGHPUT in a MIXED WORKLOAD

    try:

        def writer(delay): # Writer function, writes function into IBI_UNION table

            startime = datetime.datetime.now()

            db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2')

            cur = db.cursor()

            i=1
            time.sleep(delay) # Delays the function from starting rightaway
            while i<=1000000: # Number of Rows to be Inserted
                cur.execute('''
                    INSERT INTO IBI_UNION (SCAN_ID, SCAN_SHA, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_NBR, IOS_LOAD_DATE, MPT_LOAD_DATE, LOAD_DATE)
                    VALUES ("%i", "text1", "tx2", 1111, "text3", "text4", NOW(), NOW(), NOW())''' % (i))
                i=i+1

            endtime = datetime.datetime.now()
            #print "Time To INSERT 1000000 Rows is", endtime - startime
            Total_sec = (endtime-startime).total_seconds()
            Insert_Row = 1000000/Total_sec
            print "INSERT per second for a Writing a Single Row Is:", Insert_Row
            db.commit()
            db.close()

        def reader(delay):

            db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2')

            cur = db.cursor()

            time.sleep(delay)

            for r in range(1000001): # There will be 1000000 queries while the Write workload is being run

                startime = datetime.datetime.now()

                db.query('SELECT * FROM IBI_UNION WHERE SCAN_ID = %i' % r) # Sample Select query will select only one row of data from the table

                result = db.use_result()

                row = result.fetch_row()
                while row:
                    #print row
                    row = result.fetch_row()

                    endtime = datetime.datetime.now()
                    #print "Time To Read a Single Row Is", endtime - startime
                    Total_sec = (endtime-startime).total_seconds()
                    Single_Row = 1/Total_sec
                    print ("SELECT per second for Reading Single SCAN_ID QUERY Is: %i/10:" % r), Single_Row
            db.commit()
            db.close()

        thread.start_new_thread(writer, (0,)) # Starts the write function immediatly
        thread.start_new_thread(reader, (40,)) # stars the reader function 40 seconds later, as the read query is much faster and needs to wait for the row to be inserted to be able to read

        time.sleep(55) # Total time of the Thread function allowed
        pass # Do Nothing

    except Exception, e:
        print str(e)

def size(): # Calculates the size of the database

    ('SELECT table_schema "MemSql_tutorial", SUM( data_length + index_length) / 1024 / 1024 "125860" FROM information_schema.TABLES GROUP BY table_schema;') #The sum of the data length and the index length of MemSQL_tutorial database I am using is the size of the database

def shard(): ## Shard Query Function Using MemSQL Theoretical Demonstration
    try:
        db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2')

        cur = db.cursor()
        i = 1
        x = 1000000
        while i<=1000000:
            cur.execute('''
                INSERT INTO IBI_UNION (SCAN_ID, SCAN_SHA, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_NBR, IOS_LOAD_DATE, MPT_LOAD_DATE, LOAD_DATE)
                VALUES ("%i", "%i", "tx2", 1111, "text3", "text4", NOW(), NOW(), NOW())''' % (i,x)) # SCAN_SHA will also have a unique value like SCAN_ID
            i=i+1
            x=x+1

        startime = datetime.datetime.now()

        db.query('EXPLAIN SELECT COUNT (DISTINCT SCAN_SHA) FROM IBI_UNION') #Full Table Scan, Quite Slow, Lots of room to Optimize

        result = db.use_result()

        row = result.fetch_row()
        while row:
            row = result.fetch_row()
        endtime = datetime.datetime.now()

        Total_sec = (endtime-startime).total_seconds()
        Unsharded = 1/Total_sec # One SELECT QUERY
        print "Unsharded COUNT SELECT per second for Reading SCAN_SHA Is:", Unsharded

        startime2 = datetime.datetime.now()

        
        db.query('SELECT COUNT(distinct expr_1232563) AS `count FROM `aggregation_tmp_9527634') # 

        result2 = db.use_result()

        row2 = result.fetch_row()
        while row2:
            row2 = result2.fetch_row()
        endtime2 = datetime.datetime.now()

        Total_sec2 = (endtime2-startime2).total_seconds()
        Sharded = 1/Total_sec2
        print "Sharded COUNT SELECT per second for Reading SCAN_SHA is", Sharded

        db.commit()
        db.close()

    except Exception, e:
        print str(e)

        

def Optimize(): # Calulates the difference in Select speeds between SCAN_ID and SCAN_SHA

    try:

        db = MySQLdb.connect(host='ec2-54-173-4-170.compute-1.amazonaws.com', user='root', db='MemSQL_tutorial2')

        cur = db.cursor()

        i=1
        x=1000000
        a = 1
        b = 1000000
        c = 1000000
        d = 1# Sample VARCHAR
        while i<=1000000: # 1000000 Rows to be written
            cur.execute('''
                INSERT INTO IBI_UNION (SCAN_ID, SCAN_SHA, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_NBR, IOS_LOAD_DATE, MPT_LOAD_DATE, LOAD_DATE)
                VALUES ("%i", "%i", "tx2", 1111, "text3", "text4", NOW(), NOW(), NOW())''' % (i,x)) # SCAN_SHA will also have a unique value like SCAN_ID
            i=i+1
            x=x+1

        startime = datetime.datetime.now()

        for a in range(1000000): 

            db.query('SELECT * FROM IBI_UNION WHERE SCAN_ID = %i' % a) # Sample SELECT Query, pulling one row at a time from SCAN_ID

            result = db.use_result()

            row = result.fetch_row()
            while row:
                row = result.fetch_row()
        endtime = datetime.datetime.now()

        #print "Time To Read 1000000 SCAN_ID Rows is", endtime - startime
        Total_sec_SCAN = (endtime-startime).total_seconds()
        SCAN_SCAN_SELECT = 100/Total_sec_SCAN
        print "Unoptimized SELECT per second for Reading SCAN_ID Is:", SCAN_SCAN_SELECT

        startime2 = datetime.datetime.now() # Start Second Timer

        for b in range(1000000,2000000): # Also a Million Queries

            db.query('SELECT * FROM IBI_UNION WHERE SCAN_SHA = %i' % b) # Sample SELECT QUERY for SCAN_SHA 

            result2 = db.use_result()

            row2 = result2.fetch_row()
            while row2:
                row2 = result2.fetch_row()
                endtime2 = datetime.datetime.now()
                
        #print "Time To Read 1000000 SCAN_SHA Rows is", endtime2 - startime2
        Total_sec = (endtime2-startime2).total_seconds()
        SCAN_SHA_SELECT = 1000000/Total_sec
        print "Unoptimized SELECT per second for Reading SCAN_SHA Is:", SCAN_SHA_SELECT

        startime3 = datetime.datetime.now() # Start Third Timer

        for c in range(1000000,1000000): # Also a Million Queries

            db.query('SELECT SCAN_ID, SCAN_SHA, SCAN_TYPE, SCAN_COUNT, MACHINE_TYPE, SEQUENCE_NBR, IOS_LOAD_DATE, MPT_LOAD_DATE, LOAD_DATE FROM IBI_UNION WHERE SCAN_SHA = %i' % b) # Eliminating the astrix and writing the columns yourself increase the speed of the query per second

            result3 = db.use_result()

            row3 = result3.fetch_row()
            while row3:
                row3 = result3.fetch_row()
                endtime3 = datetime.datetime.now()
                
        #print "Time To Read 1000000 SCAN_SHA Rows is", endtime2 - startime2
        Total_sec_2 = (endtime3-startime3).total_seconds()
        SCAN_SHA_SELECT_2 = 1000000/Total_sec
        print "Optimized SELECT per second for Reading SCAN_SHA Is:", SCAN_SHA_SELECT_2

        startime4 = datetime.datetime.now()

        for d in range(1000000): 

            db.query('SELECT * FROM IBI_UNION WHERE SCAN_ID = %i' % a) ## Using the Primary SCAN_ID KEY as the Shard Key, Optimizing the query by selecting directly from the shard key

            result4 = db.use_result()

            row4 = result4.fetch_row()
            while row4:
                row4 = result4.fetch_row()
                endtime4 = datetime.datetime.now()

        #print "Time To Read 1000000 SCAN_ID Rows is", endtime - startime
        Total_sec_4 = (endtime4-startime4).total_seconds()
        SCAN_SCAN_SELECT_4 = 1000000/Total_sec_4
        print "Optimized SELECT per second for Reading SCAN_ID Is:", SCAN_SCAN_SELECT_4

        db.commit()
        db.close()

    except Exception, e:
        print str(e)


  

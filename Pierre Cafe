import csv
from collections import OrderedDict
import operator
import numpy as np

with open('sales.csv', 'rb') as f: # Opening Sales File
    r = csv.reader(f) # Read Sales file
    dict2 = {row[0]: row[1:] for row in r} # Collect data into a dictionary 

with open('products.csv', 'rb') as f: # Openaing Products File
    r = csv.reader(f) # Read Products file
    dict1 = OrderedDict((row[0], row[1:]) for row in r) # Collect data in its original order to a dictionary

result = OrderedDict() # Combining the two files together based on common product name key
for d in (dict1,dict2): # Finding the Common Products in Products and Selling Folders
    for key, value in d.iteritems(): # Extracting the two columns from the folders
        if key not in (dict1.keys() and dict2.keys()): #Protect against finding a Product Name that does not have Sales Value
            pass # Do nothing
        elif key in (dict2.keys() and dict1.keys()): #Products That have a Sales Value and a Category will be used for the calculations
            result.setdefault(key, []).extend(value) # Attach Sales value from Sales File to Product File

with open('complete.csv', 'wb') as f: # Create a CSV file called Complete
    w = csv.writer(f) # Write the complte.csv file
    for key, value in result.iteritems(): # Collect every key value pair from the two dictionaries above
        w.writerow([key] + value) # Write the Extended value from results to the complete.csv file

##################################################################### DO NOT EDIT ABOVE ################################################################################
        
def main(): # Main Function

    try: # Try loop used as best practice incase of any errors
        Product, Category, Sales = np.loadtxt('complete.csv', delimiter = ',', unpack = True, dtype = 'str') # Categorize the three columns with these Headers

        Total_Snacks = [Category == 'Snacks'] # Select only Products under the Snacks Category
        for eachProduct in Total_Snacks: # Collect all snack products
            snacks = Sales[eachProduct] # Collect all snack sales values for each product in the Snacks Category
            sum_snacks = np.array(snacks, np.float) # Converts the Snack Values from a String to a float

        Total_Beverages = [Category == 'Beverages'] # These are all the product names you gave me, just copy and paste whatever you want, and change the category name
        for eachProduct in Total_Beverages:
            beverages = Sales[eachProduct]
            sum_beverages = np.array(beverages, np.float)

        Total_Candy = [Category == 'Candy']
        for eachProduct in Total_Candy:
            candy = Sales[eachProduct]
            candy2 = np.array(Sales[eachProduct], np.float)
            sum_candy = np.array(candy, np.float)

        Total_Breakfast = [Category == 'Breakfast']
        for eachProduct in Total_Breakfast:
            breakfast = Sales[eachProduct]
            sum_breakfast = np.array(breakfast, np.float)

        Total_Baby_Food = [Category == 'Baby Food']
        for eachProduct in Total_Baby_Food:
            baby = Sales[eachProduct]
            sum_babyfood = np.array(baby, np.float)

        Total_Pantry = [Category == 'Pantry & Condiments']
        for eachProduct in Total_Pantry:
            pantry = Sales[eachProduct]
            sum_pantry = np.array(pantry, np.float)

        Total_Cooking = [Category == 'Cooking Supplies']
        for eachProduct in Total_Cooking:
            cook = Sales[eachProduct]
            sum_cooking = np.array(cook, np.float)

        Total_Vitamins = [Category == 'Vitamins & Supplements']
        for eachProduct in Total_Vitamins:
            vitamins = Sales[eachProduct]
            sum_vitamins = np.array(vitamins, np.float)

        Total_Baking = [Category == 'Baking Supplies']
        for eachProduct in Total_Baking:
            baking = Sales[eachProduct]
            sum_baking = np.array(baking, np.float)

        Total_Frozen = [Category == 'Frozen Foods']
        for eachProduct in Total_Frozen:
            frozen = Sales[eachProduct]
            sum_frozen = np.array(frozen, np.float)

        Total_Boxed = [Category == 'Boxed Meals & Sides']
        for eachProduct in Total_Boxed:
            boxed = Sales[eachProduct]
            sum_boxed = np.array(boxed, np.float)

        Total_Pet = [Category == 'Pet Food']
        for eachProduct in Total_Pet:
            pet = Sales[eachProduct]
            sum_pet = np.array(pet, np.float)

        Total_Canned = [Category == 'Canned Goods']
        for eachProduct in Total_Canned:
            canned = Sales[eachProduct]
            sum_canned = np.array(canned, np.float)
            
        Top_Categories = [['Product Name', 'Total Sales'], # Creating a Tuple which will include the product name and the sum of all their Sales Amounts
                    ['Frozen Foods', sum(sum_frozen)], # Product Category, and the sum of all its sales amounts
                    ['Snacks', sum(sum_snacks)], # Just copy and paste, and change the cateogry name if you want to add more, delete whatever one you dont want
                    ['Breakfast', sum(sum_breakfast)],
                    ['Beverages', sum(sum_beverages)],
                    ['Candy', sum(sum_candy)],
                    ['Vitamins and Supplements', sum(sum_vitamins)],
                    ['Baby Food', sum(sum_babyfood)],
                    ['Pantry and Condiments', sum(sum_pantry)],
                    ['Cooking Supplies', sum(sum_cooking)],
                    ['Baking Supplies', sum(sum_baking)],
                    ['Boxed Meals & Sides', sum(sum_boxed)],
                    ['Pet Food', sum(sum_pet)],
                    ['Canned Goods', sum(sum_canned)]]
            
        Top_Categories.sort(key=lambda tup: tup[1], reverse = True) # Sort the Tuple in Descending Order by the second value or the Sales Amount
            
        print "Top 5 Product Categories with their Sales Amount \n " # Prints the Title for the answers below
            
        x = 0 # Simple iterator, initiated at 0
        for each in Top_Categories: # Selects each Product Category and its Sales Amount from the Tuple above
            print each # Prints each Tuple seperately or a seperate line
            x+=1 # Iterates everytime the loop is run
            if x == 6: # You only wanted the top 5 categories and including the title, the loop needs to run 6 times, if you want n top products run the loop n+1 times
                break # After Loop is run 6 times and prints out top 5 categories and the title, this loop ends, eliminated the lower selling categories

            print " " # Seperates Requirement one from two
            
            #################################################### Requirement 2 ##############################################################

        Highest_Candy = [Category == 'Candy'] # Selects all Products from Candy Category
        for eachProduct in Highest_Candy: # Collects each product from the Candy Category
            Sales_Amount = Sales[eachProduct] # Collects all the sales amount for each product in the Candy Category
            Product_Name = Product[eachProduct] # Collects all the Product Names for each product in the Candy Category
            Candy_Products = zip(Product_Name, Sales_Amount) # Combines the two lists above into a tuple, attached each product name with its sales amount

        Candy_Products.sort(key=lambda tup: tup[1], reverse = True) # Sorts the tuple above in decending order by the highest sales amount first 

        print "Top Selling Candy Product with its Sales Amount \n " # Prints the the title for the answer below
        
        x = 0 # Simplete iterator, intiated at 0
        for each in Candy_Products: # Selectes each product and its sales amount from the Tuple above
            print each # Prints each products and its sales amount in the Candy Category
            x+=1 # Iterates everytime the loop is run
            if x == 1: # There is no title, and we are looking for only the highest value so the loop will break after it is run once
                break # After the loop runs once, the highest value is printed, all other products in the candy category are eliminated
        
    except Exception, e: # Completes the try and exception loop
        print str(e) # if the try loop fails e is printed

        # Just change the candy category name, to whatever you want, don't need to touch anything else. 
                  

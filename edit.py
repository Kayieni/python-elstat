from bs4 import BeautifulSoup
import requests
from requests import get
import xlrd
import mysql.connector
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import csv
import numpy as np
from translate import Translator


# --------------------------------------------------------------------------------------------------------------------------
# DOWNLOAD AND USE NECESSARY FILES BELOW
# --------------------------------------------------------------------------------------------------------------------------

# here i download the necessary excel files from the domain

# hold domain name
domain = "https://www.statistics.gr"
# hold page to drag html from
page = requests.get("https://www.statistics.gr/el/statistics/-/publication/STO04/")
# hold page's html
html0 = page.content
# throw parser into html
soup = BeautifulSoup(html0, 'html.parser')

# find all links in the above html
for link in soup.find_all("a"):
    # get the link from the link tag in html which are stored in the hypertext reference (href) attribute
    url = link.get('href')
    # if this part is contained in the href link:
    # to only download the 4th quarter which holds the info for the whole year:
    if (("/2014" in url) or ("/2013" in url) or ("/2012" in url) or ("/2011" in url)) and ("Q4" in url):
        # create file named 2011-Q4.xls for example by using the link's name
        file_name = url.split('STO04/')[1]
        # this way i grap the second part of the splited phrase and then (continue below)
        # i add to this the .xls to turn it into an excel file name
        file_name = (file_name + ".xls")
        print("NAME OF FILE: " + file_name)

        # go to the wanted page
        newpagename = domain + url          # i.e.: https://www.statistics.gr/link_of_file_into_html_of_domain_page
        pagaki = requests.get(newpagename)  # now i go to the corresponding link to download it

        # hold pagaki's html following the some procedure as above
        htmlaki = pagaki.content
        soupaki = BeautifulSoup(htmlaki, 'html.parser')

        # find the wanted file
        for coltitle in soupaki.find_all('td', class_="titleCol"):
            # hold url in the same way as before
            linkaki = coltitle.find('a')
            urlaki = linkaki.get("href")

            # get the wanted text's link
            if "ανά χώρα προέλευσης και μέσο μεταφοράς" in linkaki.text:
                # extract the wanted file into the corresponding file into a folder named downloads
                with open("downloads/" + file_name, "wb") as outfile:
                    newpage = urlaki
                    response = get(urlaki)
                    outfile.write(response.content)

            else:
                continue

    else:
        continue


# --------------------------------------------------------------------------
# here i will hold only the 3 6 9 12 sheets of each excel file (bc this is all i need)
# so i want 4 sheets for 4 years so 16 sheets total
# initialize a dictionaty to hold all the necessary info into corresponding variables
# d = {}
# enter the right info in the right dictionary key value to access easily
# for sheetnum in range(12):
#     if (sheetnum == 2) or (sheetnum == 5) or (sheetnum == 8) or (sheetnum == 11):
#         for tb in range(1,5):
#             d["s2011_{0}".format(tb)] = data11.sheet_by_index(sheetnum)
#             d["s2012_{0}".format(tb)] = data12.sheet_by_index(sheetnum)
#             d["s2013_{0}".format(tb)] = data13.sheet_by_index(sheetnum)
#             d["s2014_{0}".format(tb)] = data14.sheet_by_index(sheetnum)
#     else:
#         continue
# -----------------------------------------------------------------------------------------------
# here i will read the necessary already downloaded excel files to collect all the info i want

# here just open the 4 necessary files
# lets try with pandas
excel1 = 'downloads\\2011-Q4.xls'
excel2 = 'downloads\\2012-Q4.xls'
excel3 = 'downloads\\2013-Q4.xls'
excel4 = 'downloads\\2014-Q4.xls'
# initiate a dictionary named fck
fck = {}
# initiate a table with 0 value
tbl = 0
# basically shtnum variable is a counter for the sheets inside the downloaded excel file
for shtnum in range(12):
    # i only need the 3d month every time to collent the year's quarters.
    # So i only need March June September December, so 2 5 8 11, starting from 0
    if (shtnum == 2) or (shtnum == 5) or (shtnum == 8) or (shtnum == 11):
        # every sheet has differences so i make sure that nothing is skipped or forgotten that shouldn't
        if shtnum == 2:
            skip2 = list(range(0, 71))
            skip3 = list(range(0, 70))
            skip4 = list(range(0, 72))
        if shtnum == 5:
            skip2 = list(range(0, 71))
            skip3 = list(range(0, 70))
            skip4 = list(range(0, 73))
        if shtnum == 8:
            skip2 = list(range(0, 71))
            skip3 = list(range(0, 73))
            skip4 = list(range(0, 71))
        if shtnum == 11:
            skip2 = list(range(0, 73))
            skip3 = list(range(0, 73))
            skip4 = list(range(0, 73))

        # for tbl in range(1, 5):
        # repeate the proccess for every wanted year from 2011 to 2014
        tbl += 1
        fck["s2011_{0}".format(tbl)] = pd.read_excel(excel1, sheet_name=shtnum, header=1, index_col=0, skiprows=range(0, 71))
        fck["s2011_{0}".format(tbl)] = fck["s2011_{0}".format(tbl)].sort_values(by=['ΣΥΝΟΛΟ'], ascending=False)
        fck["s2012_{0}".format(tbl)] = pd.read_excel(excel2, sheet_name=shtnum, header=1, index_col=0, skiprows=skip2)
        fck["s2012_{0}".format(tbl)] = fck["s2012_{0}".format(tbl)].sort_values(by=['ΣΥΝΟΛΟ'], ascending=False)
        fck["s2013_{0}".format(tbl)] = pd.read_excel(excel3, sheet_name=shtnum, header=1, index_col=0, skiprows=skip3)
        fck["s2013_{0}".format(tbl)] = fck["s2013_{0}".format(tbl)].sort_values(by=['ΣΥΝΟΛΟ'], ascending=False)
        fck["s2014_{0}".format(tbl)] = pd.read_excel(excel4, sheet_name=shtnum, header=1, index_col=0, skiprows=skip4)
        fck["s2014_{0}".format(tbl)] = fck["s2014_{0}".format(tbl)].sort_values(by=['ΣΥΝΟΛΟ'], ascending=False)
    else:
        continue


# --------------------------------------------------------------------------------------------------------------------------
# DATABASE CONNECTION, DATA STORING AND RESULT VERIFICATION BELOW
# --------------------------------------------------------------------------------------------------------------------------

# create the database connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Manou",
    db="statistics"
)

# print (mydb)

# create database if it does not already exists
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS statistics")

# use the created database
mycursor.execute("USE statistics")

# drop tables to avoid dublicate inserts
mycursor.execute("DROP TABLE IF EXISTS 1_total_arrivals")
mycursor.execute("DROP TABLE IF EXISTS 2_top_country")
mycursor.execute("DROP TABLE IF EXISTS 3_transportation")
mycursor.execute("DROP TABLE IF EXISTS 4_quarter_arrivals")

# create tables to hold the info for each excel file
# note: here, the "if not exists" can be emitted but why not?
mycursor.execute("CREATE TABLE IF NOT EXISTS 1_total_arrivals(year INT, total INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS 2_top_country(year INT, country VARCHAR(20), total INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS 3_transportation(year INT, air INT, train INT, ship INT, car INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS 4_quarter_arrivals(year INT, q1 INT, q2 INT, q3 INT, q4 INT)")

# -------------------------------------------------------------------------------------------------
# print erwtimata + insert into database

sum = 0


# erwthma 1:geniko etisio synolo
print("----------------------------------------------------------------\nQuestion 1:\n___________\n")
for year in range(1, 5):
    total = fck["s201{0}_4".format(year)]['ΣΥΝΟΛΟ'][0]
    print("Total for year 201" + str(year) + " : \t" + str(round(total)))
    print("")
    sum += total

    query1 = """INSERT INTO 1_total_arrivals (year, total) VALUES (%s, %s)"""
    year1 = 2010 + year
    total1 = int(round(total))
    # assign value
    values1 = (year1, total1)
    # execute query
    mycursor.execute(query1, values1)

print("\nFinal sum of the 2011-2015 Period:")
print(int(round(sum)))
print("")


# erwthma 2: xwres me perissoteres afikseis
print("----------------------------------------------------------------\nQuestion 2:\n___________\n")
for year in range(1, 5):
    top5 = fck["s201{0}_4".format(year)].iloc[3:9]
    # print(top5)
    # print("")
# exit()
    name = top5['Unnamed: 1'][0]
    total = round(top5['ΣΥΝΟΛΟ'][0])
    print("Year: 201" + str(year) + "\tName: " + name + "\tTotal: " + str(total) + "\n")

    query2 = """INSERT INTO 2_top_country(year, country, total) VALUES (%s, %s, %s)"""
    year2 = 2010 + year
    country2 = name
    total2 = total
    # assign value
    values2 = (year2, country2, total2)
    # execute query
    mycursor.execute(query2, values2)


# erwthma 3: ana meso metaforas
print("\n--------------------------------------------------------------\nQuestion 3:\n___________\n")
for year in range(1, 5):
    alltotals = fck["s201{0}_4".format(year)].iloc[0]
    ttlair = alltotals[1]
    ttltrain = alltotals[2]
    ttlship = alltotals[3]
    ttlcar = alltotals[4]

    print("\nYear 201" + str(year) + ":")
    print("-> By airplane: " + str(round(ttlair)))
    print("-> By train: \t" + str(round(ttltrain)))
    print("-> By ship: \t" + str(round(ttlship)))
    print("-> By car: \t" + str(round(ttlcar)))
    print("")

    query3 = """INSERT INTO 3_transportation(year, air, train, ship, car) VALUES (%s, %s, %s, %s, %s)"""
    year3 = 2010 + year
    air3 = round(ttlair)
    train3 = round(ttltrain)
    ship3 = round(ttlship)
    car3 = round(ttlcar)
    # assign value
    values3 = (year3, air3, train3, ship3, car3)
    # execute query
    mycursor.execute(query3, values3)


# erwthma 4: ana triminia
print("----------------------------------------------------------------\nQuestion 4:\n___________\n")
for year in range (1, 5):
    lasttotal = 0
    print("Year 201" + str(year) + ":")
    for quarter in range(1, 5):
        qsum = fck["s201{0}_{1}".format(year, quarter)]['ΣΥΝΟΛΟ'][0]
        currenttotal = qsum - lasttotal
        lasttotal += currenttotal
        print("-> Total of Quarter number " + str(quarter) + ": \t" + str(round(currenttotal)))
        if quarter == 1:
            q1 = round(currenttotal)
        if quarter == 2:
            q2 = round(currenttotal)
        if quarter == 3:
            q3 = round(currenttotal)
        if quarter == 4:
            q4 = round(currenttotal)

    print("")
    query4 = """INSERT INTO 4_quarter_arrivals(year, q1, q2, q3, q4) VALUES (%s, %s, %s, %s, %s)"""
    year4 = 2010 + year
    values4 = (year4, q1, q2, q3, q4)
    mycursor.execute(query4, values4)

# print(list(fck["s2014_4"].columns))

# --------------------------------------------------------------------------------------------------------------------------

# check if everything ok by printing the abore results from the database
print("\n----------------------------------------------------------------\n--> T A B L E   1 :")
sql1 = "SELECT * FROM 1_total_arrivals"
mycursor.execute(sql1)
table1 = mycursor.fetchall()
for x in table1:
    print(x)

print("--> T A B L E   2 :")
sql2 = "SELECT * FROM 2_top_country"
mycursor.execute(sql2)
table2 = mycursor.fetchall()
for x in table2:
    print(x)

print("--> T A B L E   3 :")
sql3 = "SELECT * FROM 3_transportation"
mycursor.execute(sql3)
table3 = mycursor.fetchall()
for x in table3:
    print(x)

print("--> T A B L E   4 :")
sql4 = "SELECT * FROM 4_quarter_arrivals"
mycursor.execute(sql4)
table4 = mycursor.fetchall()
for x in table4:
    print(x)

print("\n----------------------------------------------------------------\n")
# create the csv by using the data in database
read1 = pd.read_sql(sql1, mydb)
df1 = read1.to_csv('csv/Table1.csv', index=False, header=["YEAR", "TOTAL"])
read2 = pd.read_sql(sql2, mydb)
df2 = read2.to_csv('csv/Table2.csv', index=False, header=["YEAR", "COUNTRY", "ARRIVALS"])
read3 = pd.read_sql(sql3, mydb)
df3 = read3.to_csv('csv/Table3.csv', index=False, header=["YEAR", "BY_AIRPLANE", "BY_TRAIN", "BY_SHIP", "BY_CAR"])
read4 = pd.read_sql(sql4, mydb)
df4 = read4.to_csv('csv/Table4.csv', index=False, header=["YEAR", "first_QUARTER", "second_QUARTER", "third_QUARTER", "forth_QUARTER"])

# print csvs to check if all ok


# close connection with database
mydb.close()


# --------------------------------------------------------------------------------------------------------------------------
# CREATE THE PLOTS BELOW
# --------------------------------------------------------------------------------------------------------------------------

# create plot for exercise 1 synola
print("Figure 1 is getting ready. Please wait...")
csv1 = pd.read_csv("csv/Table1.csv")
x = csv1.YEAR
y = csv1.TOTAL

X = np.arange(4)
figure1 = plt.figure("Figure 1")
ax = figure1.add_axes([0.1, 0.1, 0.8, 0.8])
ax.set_ylabel('Total Arrivals')
ax.set_xlabel('Year')
ax.set_title('Arrivals for 2011-2015')
ax.set_xticks(np.arange(2010, 2015, 1))
ax.set_yticks(np.arange(0, 30000000, 1000000))
ax.bar(x, y)
for i in range(4):
    plt.annotate(y[i], xy=(x[i], y[i]), xytext=(x[i]-0.25, y[i]+100000))
    # 1o orisma: ti tha fainetai sto label / xy : poy deixnei to text / xytext: pou topotheteitai to text
plt.savefig('plots/exercise1.png')
plt.show()
plt.close()
print("\tFigure 1 COMPLETED\n")


# create plot for exercise 2 ana prwth xwra
print("Figure 2 is getting ready. Please wait...")
translator = Translator(from_lang="greek", to_lang="english")
csv2 = pd.read_csv("csv/Table2.csv")
x = csv2.COUNTRY
y = csv2.ARRIVALS
z = csv2.YEAR
X = np.arange(4)
figure2 = plt.figure("Figure 2")
ax = figure2.add_axes([0.1, 0.1, 0.8, 0.8])
ax.set_ylabel('Total Arrivals')
ax.set_xlabel('Year')
ax.set_title('Top Country for Arrivals in 2011-2015')
ax.set_xticks(np.arange(2010, 2015, 1))
ax.set_yticks(np.arange(0, 30000000, 100000))
ax.bar(z, y, color='r')
for i in range(4):
    if i == 3:
        plt.annotate(translator.translate(x[i]), xy=(z[i], y[i]), xytext=(z[i]-0.75, y[i]+10000))
    else:
        plt.annotate(translator.translate(x[i]), xy=(z[i], y[i]), xytext=(z[i]-0.25, y[i]+10000))
plt.savefig('plots/exercise2.png')
plt.show()
plt.close()
print("\tFigure 2 COMPLETED\n")


# create plot for exercise 3 ana meso metaforas
print("Figure 3 is getting ready. Please wait...")
csv3 = pd.read_csv("csv/Table3.csv")
a = csv3.BY_AIRPLANE
t = csv3.BY_TRAIN
s = csv3.BY_SHIP
c = csv3.BY_CAR
y = csv3.YEAR
X = np.arange(4)
figure3 = plt.figure("Figure 3")
ax = figure3.add_axes([0.1, 0.1, 0.8, 0.8])
ax.set_ylabel('Total Arrivals')
ax.set_xlabel('Year')
ax.set_title('Arrivals in 2011-2015 by mean of transport')
ax.set_xticks(np.arange(2010, 2015, 1))
ax.set_yticks(np.arange(0, 30000000, 1000000))
ax.bar(y + 0.00, a, color='r', width=0.25)
ax.bar(y + 0.25, t, color='g', width=0.25)
ax.bar(y + 0.50, s, color='b', width=0.25)
ax.bar(y + 0.75, c, color='y', width=0.25)
for i in range(4):
    plt.annotate(a[i], xy=(y[i], a[i]), xytext=(y[i]-0.25, a[i]+100000))
    plt.annotate(t[i], xy=(y[i], t[i]), xytext=(y[i]+0.105, t[i]+100000))
    plt.annotate(s[i], xy=(y[i], s[i]), xytext=(y[i]+0.25, s[i]+100000))
    plt.annotate(c[i], xy=(y[i], c[i]), xytext=(y[i]+0.5, c[i]+100000))
ax.legend(labels=['Airplane', 'Train', 'Ship', 'Car'], loc='upper left', ncol=2)
plt.savefig('plots/exercise3.png')
plt.show()
plt.close()
print("\tFigure 3 COMPLETED\n")


# create plot for exercise 4 ana trimino
print("Figure 4 is getting ready. Please wait...")
csv4 = pd.read_csv("csv/Table4.csv")
y = csv4.YEAR
a = csv4.first_QUARTER
b = csv4.second_QUARTER
c = csv4.third_QUARTER
d = csv4.forth_QUARTER
X = np.arange(4)
figure4 = plt.figure("Figure 4")
ax = figure4.add_axes([0.1, 0.1, 0.8, 0.8])
ax.set_ylabel('Total Arrivals')
ax.set_title('Arrivals in Greece during 2011-2015 by Quarters')
ax.set_xlabel('Year')
ax.set_xticks(np.arange(2010, 2015, 1))
ax.set_yticks(np.arange(0, 30000000, 1000000))
ax.bar(y + 0.00, a, color = 'r', width=0.25)
ax.bar(y + 0.25, b, color = 'g', width=0.25)
ax.bar(y + 0.50, c, color = 'b', width=0.25)
ax.bar(y + 0.75, d, color='y', width=0.25)
for i in range(4):
    plt.annotate(a[i], xy=(y[i], a[i]), xytext=(y[i]-0.25, a[i]+100000))
    plt.annotate(b[i], xy=(y[i], b[i]), xytext=(y[i], b[i]+100000))
    plt.annotate(c[i], xy=(y[i], c[i]), xytext=(y[i]+0.25, c[i]+100000))
    plt.annotate(d[i], xy=(y[i], d[i]), xytext=(y[i]+0.5, d[i]+100000))
ax.legend(labels=['First Q.', 'Second Q.', 'Third Q.', 'Forth Q.'], loc='upper left', ncol=2)
plt.savefig('plots/exercise4.png')
plt.show()
plt.close()
print("\tFigure 4 COMPLETED")

# --------------------------------------------------------------------------------------------------------------------------
#lets now print some THANKYOUBYE messages

print("\n----------------------------------------------------------------\n")
print("""Project completed! You can find everything as described below:
        -the excel files in path: downloads/
        -the csv files in path:   csv/
        -the figures in path:     plots/
       """)
print("----------------------------------------------------------------\n")
print(">>>>>> Thanks \'n\' byeeeeeeee :)) <<<<<<")


# --------------------------------------------------------------------------------------------------------------------------
# NOTES SECTION BELOW
# --------------------------------------------------------------------------------------------------------------------------

# erwthma 1:
#   synolikes afikseis: thelw mono to teliko synoliko count tou etous
#   ara thelw ena pinaka pou na pairnei mono to etos kai to teliko synolo apo to antistixo 201X-Q4 to sheet 12
# erwthma 2:
#   xwres katagogis me perissotera arrivals: thelw thn prwth xwra kathe xrono ordered by plhthos touristwn apo ayth
#   ara tha exw ena pinaka gia kathe etos pou tha gemizei apo 12o sheet kathe q4
#   kathenas tha exei orismata tis xwres kai to plhthos twn touristwn
#   tha kanw order by plhthos, pairnw to panw panw apo olous tous pinakes
# erwthma 3:
#   ana meso metaforas: pairnw telika synola gia metaforika apo 12 sheet gia synolo etous
#   ara o pinakas tha exei etos, byair, bytrain, byship, bycar synola
# erwthma 4:
#   ana trimino: pairnw apo q4 sheets 3 6 9 12 to synolo se ola ta eth
#   ara o pinakas tha exei etos, triminia, kai synolo afiksewn

# synolika poia sheets thelw:
#   thelw ola ta eth to teleytaio sheet (12o)
#   thelw oles tis triminies ara thelw sheet (3 6 9 12)

# ti xreiazomai gia apothikefsi sth database apo ta parapanw sheet:
#   geniko synolo gia etos
#   xwres kai afikseis gia na taksinomhsw argotera
#   etisio synolo gia kathe meso
#   genko synolo gia kathe triminia








import pandas as pd
import mechanicalsoup as ms
import sqlite3

url = "https://en.wikipedia.org/wiki/William_Edward_Green"
browser = ms.StatefulBrowser()
browser.open(url)

td = browser.page.find_all("td")
columns = [value.text.strip() for value in td]
start_index = columns.index("6 January 1918 @ 1200 hours") - 1
columns = columns[start_index:len(columns) - 1]
print(columns)

column_names = ["Number",
                "Date/time",
                "Aircraft",
                "Foe",
                "Result",
                "Location",
                "Notes"]


dict = {}
for idx, key in enumerate(column_names):
    dict.update({key: columns[idx::len(column_names)]})


df = pd.DataFrame(dict)

connection = sqlite3.connect("aerial_victories.db")

cursor = connection.cursor()
linux_distributions = df.to_sql("aerial_victories", connection, if_exists="replace")

connection.close()

from urllib.request import urlopen
import psycopg2
from bs4 import BeautifulSoup
import pandas as pd
from ingest_to_postgres import connect_to_postgres, create_table


def url_scraper():
    years = (2019, 2020, 2021)

    url = ""

    for year in years:
        url = "https://www.pro-football-reference.com/years/{}/fantasy.htm".format(year)
        html = urlopen(url)
        soup = BeautifulSoup(html, features="html.parser")
        headers = [th.getText() for th in soup.findAll('tr')[1].findAll('th')] 
        headers = headers[1:] 
        #print(headers[:5])  
        rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead") 
        player_stats = [[td.getText() for td in rows[i].findAll('td')] 
                for i in range(len(rows))] #for each row
        player_stats = player_stats[2:]
        #print(player_stats)
        nth = 2

        stats = pd.DataFrame(player_stats, columns = headers)
        stats = stats.replace(r'', 0, regex=True) 
        stats.columns = [x.lower().lstrip("_").replace("id","_id").replace(" ","_").replace("-","_").replace("y/a","yards_per_attempt").replace("y/r","yards_per_reception").replace("2","two_"). replace("att", "rush_att", nth).replace("rush_att","pass_att", nth-1) for x in stats.columns]
        stats.columns = [x.replace("rush_att","pass_att", 1) for x in stats.columns]
        stats['Year'] = year 

        replacements = {
            'object': 'varchar',
            'float64': 'float',
            'int64': 'int',
            'datetime64': 'timestamp',
            'timedelta64[ns]': 'varchar'
        }

        table_name = 'nfl_fantasy_stats' + str(year)
        csv_file_name = "./csv_files/nfl_fantasy_stats_{}.csv".format(year)
        col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(stats.columns, stats.dtypes.replace(replacements)))

        stats.to_csv(csv_file_name) #add your desired path prior within the string here
        
        return table_name, col_str, csv_file_name
        

if __name__ == '__main__':
    table_name, columns, csv_file_name = url_scraper()
    conn = connect_to_postgres()
    curr = conn.cursor()
    create_table(curr, table_name, columns)

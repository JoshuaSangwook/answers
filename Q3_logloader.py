import json
import sys
import sqlite3

# ===============================================
def main(): 
    conn = sqlite3.connect("./testdb")
    try:
        cur = conn.cursor()
        
        with open("lighthouse-logs.log", 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line[0] == '{':
                    j = json.loads(line)
                    userGroup = ''
                    experiment = ''
                    #index = 0
                    if 'control for' in j['msg']:
                        index = j['msg'].find("MS-")
                        experiment = j['msg'][index:index+6]
                        userGroup = 'CONTROL'
                    elif 'test for' in j['msg']:
                        index = j['msg'].find("MS-")
                        experiment = j['msg'][index:index+6]
                        userGroup = 'TEST'
                    else:
                        continue
                    data = (userGroup, experiment, j['time'], j['time'][:10])
                    sql = "insert into ExperimentLog (userGroup, experiment, time, date) values (?, ?, ?, ?)"
                    cur.execute(sql, data)
        
        conn.commit()
        
    except Exception as e:
        print('Exception: [%s]' % e)
        sys.exit(1)   
    finally:
        if conn is not None:
            conn.close() 


# ===============================================
if __name__ == "__main__":
    main()


import psycopg2
import datetime
import os
import csv
from ftp_dowload import dowload_new_payments



def update_fact_payments():
    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
    write_cursor = write_conn.cursor()


    #Загрузка новых файлов
    dowload_new_payments()


    directory = 'payments/'
    files = os.listdir(directory)

    wrong_lines = []

    write_cursor.execute("SELECT MAX(transaction_id) FROM fact_payments")
    res = write_cursor.fetchall()
    if res != [(None,)]:
        transaction_id = res[-1][0] + 1
    else:
        transaction_id = 0
        
    # print(files)
    for file in files:
        with open('payments/'+file, newline='') as f:
            spamreader = csv.reader(f)
            for row in spamreader:
                try:
                    #выборка данных
                    data = row[0].split('\t')
                    card_num = int(data[1])
                    transaction_amt = float(data[2])
                    transaction_dt = datetime.datetime.strptime(data[0], '%d.%m.%Y %H:%M:%S')
                    # print((transaction_id, card_num, transaction_amt, transaction_dt))

                    
                    #запись данных
                    write_cursor.execute('INSERT INTO fact_payments VALUES(%s, %s, %s, %s);',
                        (transaction_id, card_num, transaction_amt, transaction_dt))
                    write_conn.commit()
                    
                    transaction_id += 1
                except Exception as e:
                    wrong_lines.append((transaction_id, card_num, transaction_amt, transaction_dt))
                    
        os.remove('payments/'+file)


    #возможно стоити перенести в другое место и добавить проверку на ошибки
    if files != []:
        with open('last_payment.txt', 'w') as f:
            f.write(files[-1])


    write_cursor.close()
    write_conn.close()
    
    if wrong_lines != []:
        print('Problems with files:' + '\n'.join(wrong_lines))
        
    return wrong_lines
    
    
if __name__ == '__main__':
    update_fact_payments()
import psycopg2


def update_dim_drivers():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password='etl_tech_user_password', host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()

    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()

    # выбор только новых записей
    with open('last_read_line_driver.txt', 'r') as f:
        last_read_line_num = f.readline()
        if last_read_line_num == '':
            last_read_line_num = 0
        else:
            last_read_line_num = int(last_read_line_num)
    read_cursor.execute(f"SELECT * FROM main.drivers")
    drivers = read_cursor.fetchall()[last_read_line_num:]

    #получение маскимального id записи  
    write_cursor.execute("SELECT MAX(personnel_num) FROM dim_drivers")
    res = write_cursor.fetchall()
    if res != [(None,)]:
        personnel_num = res[-1][0] + 1
    else:
        personnel_num = 0

    #запись новых данных
    for driver in drivers:
        last_read_line_num += 1
        
        last_name = driver[2]
        first_name = driver[1]
        middle_name = driver[3]
        birth_dt = driver[7]
        card_num = driver[5]
        driver_license_num = driver[0]
        driver_license_dt = driver[4]
        delited_flag = 'N'
        end_dt = None
        
        
        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_drivers WHERE driver_license_num = '{driver_license_num}';")
        carq = write_cursor.fetchall()
        if len(carq) > 0:
            last_line = carq[-1]
            if last_line[2:] == (last_name, first_name, middle_name, birth_dt, card_num,\
                                driver_license_num, driver_license_dt, delited_flag, end_dt):
                continue
            else:
                # print((personnel_num, last_name, first_name, middle_name, birth_dt,\
                #         card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
                write_cursor.execute(f"UPDATE dim_drivers SET end_dt = CURRENT_TIMESTAMP WHERE driver_license_num = '{driver_license_num}' AND end_dt IS NULL;")
        
        write_cursor.execute('INSERT INTO dim_drivers VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                    (personnel_num, last_name, first_name, middle_name, birth_dt,\
                        card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
        # print((personnel_num, last_name, first_name, middle_name, birth_dt,\
        #                 card_num, driver_license_num, driver_license_dt, delited_flag, end_dt))
        personnel_num += 1
    write_conn.commit()

    with open('last_read_line_driver.txt', 'w') as f:
        f.write(str(last_read_line_num))

    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    
    
if __name__ == '__main__':
    update_dim_drivers()
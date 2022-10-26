import psycopg2


def update_dim_cars():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password='etl_tech_user_password', host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()


    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    # выбор только новых записей
    with open('last_read_line_car.txt', 'r') as f:
            last_read_line_num = f.readline()
            if last_read_line_num == '':
                last_read_line_num = 0
            else:
                last_read_line_num = int(last_read_line_num)
    read_cursor.execute("SELECT * FROM main.car_pool")
    cars = read_cursor.fetchall()[last_read_line_num:]


    for car in cars:
        plate_num = car[0]
        model_name = car[1]
        revision_dt = car[2]
        deleted_flag = car[4]
        end_dt = None

        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_cars WHERE plate_num = '{plate_num}';")
        update_car = write_cursor.fetchall()
        if len(update_car) > 0:
            last_line = update_car[-1]
            
            if (tuple([last_line[0]])+last_line[2:]) == (plate_num, model_name, revision_dt, deleted_flag, end_dt):
                continue
            else:
                write_cursor.execute(f"UPDATE dim_cars SET end_dt = CURRENT_TIMESTAMP WHERE plate_num = '{plate_num}' AND end_dt IS NULL;")
        
        write_cursor.execute('INSERT INTO dim_cars VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s, %s);',
                    (plate_num, model_name, revision_dt, deleted_flag, end_dt))
        # print((plate_num, model_name, revision_dt, deleted_flag, end_dt))
        last_read_line_num += 1
        write_conn.commit()

    with open('last_read_line_car.txt', 'w') as f:
        f.write(str(last_read_line_num))

    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    

if __name__ == '__main__':
    update_dim_cars()
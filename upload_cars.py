import psycopg2
import datetime


def update_dim_cars():
    read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                            password='etl_tech_user_password', host='de-edu-db.chronosavant.ru', sslmode='require')
    read_cursor = read_conn.cursor()


    write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                            password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru', sslmode='require')
    write_cursor = write_conn.cursor()


    # выбор только новых записей
    with open('last_update_dt_cars.txt', 'r') as f:
        max_update_dt = f.readline()
        if max_update_dt == '':
            max_update_dt = datetime.datetime(2004, 9, 29)
        else:
            max_update_dt = datetime.datetime.strptime(max_update_dt, '%Y-%m-%d %H:%M:%S')

    read_cursor.execute(f"SELECT * FROM main.car_pool WHERE update_dt > '{max_update_dt}'")
    cars = read_cursor.fetchall()
    

    for car in cars:
        max_update_dt = max(max_update_dt, car[5]) # Максимальное dt обновденных записей
        
        plate_num = car[0]
        model_name = car[1]
        revision_dt = car[2]
        deleted_flag = car[4]
        end_dt = None

        # Проверка на поворение строки и обновление end_dt если есть повторы
        write_cursor.execute(f"SELECT * FROM dim_cars WHERE plate_num = '{plate_num}' AND end_dt IS NULL;")
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
        write_conn.commit()

    
    with open('last_update_dt_cars.txt', 'w') as f:
        f.write(str(max_update_dt))

    write_cursor.close()
    write_conn.close()
    read_cursor.close()
    read_conn.close()
    
    return 'OK'
    

if __name__ == '__main__':
    update_dim_cars()
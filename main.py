from upload_drivers import update_dim_drivers
from upload_payments import update_fact_payments
from upload_waybills import update_fact_waybills
from upload_cars import update_dim_cars
from upload_clients import update_dim_clients
from upload_rides import update_fact_rides
import schedule
import time


def update():
    print(f'[{time.ctime()}] Upload start')
    # 1. payments
    print('fact_payments start update')
    if update_fact_payments() == []:
        print('fact_payments succesfully updated')
        
    # 2. clients
    print('Dim_clients start update')
    if update_dim_clients() == 'OK':
        print('Dim_cients succesfully updated')
        
    # 3. drivers
    print('Dim_drivers start update')
    if update_dim_drivers() == 'OK':
        print('Dim_drivers succesfully updated')

    # 4. cars
    print('dim_cars start update')
    if update_dim_cars() == 'OK':
        print('dim_cars succesfully updated')
        
    # 5. waybills
    print('fact_waybills start update')
    if update_fact_waybills() == []:
        print('fact_waybills succesfully updated')
        
    # 6. rides
    print('fact_rides start update')
    if update_fact_rides() == []:
        print('fact_rides succesfully updated')


    print(f'[{time.ctime()}] Upload end')
    return

update()
#запуск загрузки данных каждые 24 часа
schedule.every(24).hours.do(update)
while True:
    schedule.run_pending()
    time.sleep(1)
from ftplib import FTP_TLS
import os


def connection(func):
    def _wrapper(*args, **kwargs):
        con = FTP_TLS('de-edu-db.chronosavant.ru', 'etl_tech_user', 'etl_tech_user_password')
        con.prot_p()
        func(con)
        con.close()
    return _wrapper


def download(way, files):
    error_files = []
    for i in range(len(files)//200+1):
        con = FTP_TLS('de-edu-db.chronosavant.ru', 'etl_tech_user', 'etl_tech_user_password')
        con.prot_p()
        
        con.cwd(way)
        
        for file in files[i*200:200*(i+1)]:
            try:
                # print(file)
                with open(way+file, 'wb') as f:
                    con.retrbinary('RETR ' + file, f.write, 1024)
            except:
                os.remove(way+file)
                print("ERR:" + file)
                error_files.append(file)
        con.close()
    return error_files


@connection
def dowload_all_waybills(con):
    con.cwd('/waybills')
    
    files = con.nlst()
    error_files = download('waybills/', files)
    print(error_files)


@connection
def dowload_new_waybills(con):
    con.cwd('/waybills')
    
    with open('last_waybill.txt', 'r') as f:
        last_wb = f.readline()
    
    new_files = [i for i in con.nlst() if i > last_wb]
    
    error_files = download('waybills/', new_files)
    
    if error_files == []:
        return 'OK'
    else:
        print(f'ERROR files: {" ".join(error_files)}')
        return error_files
    

@connection
def dowload_all_payments(con):
    con.cwd('/payments')

    files = con.nlst()
    error_files = download('payments/', files)
    print(error_files)
            
            
@connection
def dowload_new_payments(con):
    con.cwd('/payments')
    
    with open('last_payment.txt', 'r') as f:
        last_pay = f.readline()
    
    new_files = [i for i in con.nlst() if i > last_pay]
    
    error_files = download('payments/', new_files)
    
    if error_files == []:
        return 'OK'
    else:
        print(f'ERROR files: {" ".join(error_files)}')
        return error_files
    
    
if __name__ == "__main__":
    # dowload_all_waybills()
    # dowload_all_payments()
    dowload_new_waybills()
    # dowload_new_payments()
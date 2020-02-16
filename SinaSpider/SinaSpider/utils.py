import psycopg2

def get_db_conn():
	return psycopg2.connect(user='postgres',password='Ye25554160',database='sina_page',host='127.0.0.1',port=5432)

import psycopg2
from config import config

class ml_db:
    def __init__(self, table_name):
        self.conn_ = psycopg2.connect(**config(section="ml"))
        self.table_name_ = table_name
            
    def __del__(self):
        if self.conn_ is not None:
            self.conn_.close()

    def closes_t0_t1(self, index):
        cur = self.conn_.cursor()
        query = 'SELECT idx, f4 FROM "data"."' + self.table_name_ + '" WHERE idx >= ' + str(index) + ' ORDER BY idx ASC LIMIT 2'
        cur.execute(query)
        result = [cur.fetchone(), cur.fetchone()]
        cur.close()
        return result

class ml_result_db:
    def __init__(self):
        self.conn_ = psycopg2.connect(**config(section="ml_result"))
            
    def __del__(self):
        if self.read_cur_ is not None:
            self.read_cur_.close()
        if self.conn_ is not None:
            self.conn_.close()

    def query_all(self):
        self.read_cur_ = self.conn_.cursor()
        self.read_cur_.execute('SELECT id, model_descr_id, idx FROM "public"."prediction_result"')
        return self.read_cur_

    def query_last(self, count):
        self.read_cur_ = self.conn_.cursor()
        self.read_cur_.execute('SELECT id, model_descr_id, idx FROM "public"."prediction_result" ORDER BY idx DESC LIMIT %s', [count])
        return self.read_cur_
    
    def update_t0_t1(self, id, t0, t1):
        write_cur = self.conn_.cursor()
        write_cur.execute('UPDATE "public"."prediction_result" SET t0 = %s, t1 = %s WHERE id = %s', [t0, t1, id])
        self.conn_.commit()
        write_cur.close()

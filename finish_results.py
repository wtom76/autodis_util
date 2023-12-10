import psycopg2
from config import config
from db_work import ml_db, ml_result_db

try:
    ml_23 = ml_db('000001')
    ml_45 = ml_db('000004')
    mlr = ml_result_db()
    
    for id, model_descr_id, idx in mlr.query_last(2):

        if id is None:
            break
        
        if model_descr_id in (2, 3):
            t0_t1 = ml_23.closes_t0_t1(idx)
        elif model_descr_id in (4, 5):
            t0_t1 = ml_45.closes_t0_t1(idx)
        else:
            continue

        print(t0_t1)
        if len(t0_t1) < 2 or t0_t1[0] is None or t0_t1[1] is None:
            continue

        actual_idx = t0_t1[0][0]
        actual_t0 = t0_t1[0][1]
        actual_t1 = t0_t1[1][1]
        
        if actual_idx != idx or actual_t0 is None or actual_t1 is None or actual_t0 == 0 or actual_t1 == 0:
            continue
        
        mlr.update_t0_t1(id, actual_t0, actual_t1)

except (Exception, psycopg2.DatabaseError) as error:
    print(error)

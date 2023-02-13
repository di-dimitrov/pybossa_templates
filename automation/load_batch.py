import os
import psycopg2

def load_new_data(batch_size,exec_file, dest_file, project_id, phase,project_file='project.json'):
    connection = psycopg2.connect(user="root",password="1234",host="127.0.0.1",port="5432",database="pybossa")
    try:
        cursor = connection.cursor()
        query = "select id from task where project_id = %s and priority_0 = 0 and state = 'completed' order by id ASC limit %s;"
        cursor.execute(query,(project_id, batch_size))
        results = cursor.fetchall()
        from_id = results[0][0]         
        to_id = results[-1][0]
        if phase == 'anno':
            n_annotators = 1
            redundancy = 2
        if phase == 'cons':
            n_annotators = 2
            redundancy = 1
        cursor.execute("update task set priority_0 = 1 where id >= %s and id <= %s" % (from_id,to_id))
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        raise("Error fetching data from PostgreSQL table", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed \n")
    print(from_id)
    print(to_id)
    print(n_annotators) 
    os.system('PYTHONIOENCODING=utf-8 python3 %s %s %s -1 %s %s %s %s' % (exec_file, dest_file, project_id, n_annotators, phase, from_id, to_id))
    os.system('export LC_ALL=C.UTF-8')
    os.system('export LANG=C.UTF-8')
    os.system('LC_ALL=C.UTF-8 \
               LANG=C.UTF-8 \
               pbs --project %s add-tasks --tasks-file %s --redundancy %s' % (project_file,dest_file, redundancy))
load_new_data(3,'result_extractor_merged.py', 'latest_result.json',str(40), 'cons')
load_new_data(3,'result_extractor_merged.py', 'latest_result.json',str(39), 'cons')
#load_new_data(3,'merged_annotation/result_extractor_merged.py', 'latest_result.json',str(40), 'cons')
#load_new_data(3,'merged_consolidation/result_extractor_merged.py', 'latest_result.json',str(39), 'cons')

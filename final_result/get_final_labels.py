import os
import sys
import json
import psycopg2
from collections import Counter
from collections import OrderedDict
import operator
from datetime import date


prettyPrint = False

def extract_final_results(project_id,fromm,to,isPretty):
    global prettyPrint
    prettyPrint = isPretty
    connection = psycopg2.connect(user="root",password="1234",host="127.0.0.1",port="5432",database="pybossa")
    connection.set_client_encoding('UTF8')
    try:
        cursor = connection.cursor()
        query = "select distinct on(task_id)task_id, t.info->>'image', tr.info, t.info->>'text' from task_run tr inner join task t on t.id = tr.task_id where tr.project_id = %s and task_id >= %s and task_id <= %s order by task_id,finish_time desc;"
        cursor.execute(query,[project_id,fromm,to]);
        results = cursor.fetchall()
        extract_json_tasks(results)
        #extract_label_for_each_img(results)
    except (Exception, psycopg2.Error) as error:
        raise("Error fetching data from PostgreSQL table", error)
    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed \n")
def extract_json_tasks(results):
    final_json = []
    for row in results:
        text_spans = []
        current_object = {}
        current_object['id'] = row[1]
        current_object['text'] = row[3]
        current_object['image'] = row[1]
        print(row[2])
        if 'answers' in row[2]:
            text_spans = load_field_escaped_correctly(row[2]['answers'])
        else:
            text_spans = load_field_escaped_correctly(row[2]['text_tech_span'])
            current_object['technique_selection'] = load_field_escaped_correctly(row[2]['text_techniques_anno']) + load_field_escaped_correctly(row[2]['image_techniques_anno'])
        for anno in text_spans:
            anno['text_fragment'] = anno['text']
            anno.pop('text')
        current_object['text_spans'] = text_spans
        final_json.append(current_object)
    print(final_json)
    save_task(final_json, 'extracted_labels',prettyPrint)
def load_field_escaped_correctly(field):
    temp = ''
    if not type(field) is str:
        temp = json.loads(json.dumps(field,ensure_ascii=False).encode('utf8'))
    else:
        temp = json.loads(field)
    return temp;
def get_id(row):
    tmp = row[1].split('_')[0]
    id = row[1].split('.')[0].split('image')
    if(len(id) > 1):
        id = tmp + id[1]
    else:
        id = tmp 
    return id 
def create_list_of_needed_images(results):
    list_images = []
    for row in results:
        list_images.append(row[1])
    save_task(list_images,'list_of_images_for_json',False)
def save_task(final_results, filename,pretty):
    today = date.today()
    curr_time = today.strftime("%Y_%m_%d")
    THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
    new_file = os.path.join(THIS_FOLDER, filename + '_' + curr_time + '.txt')
    tasks = open(new_file,"w",encoding='utf8')
    #tasks.write(json.dumps(final_results,indent=4, separators=(',', ': '))) #.replace('\\"','\"')
    if pretty:
        tasks.write(json.dumps(final_results,indent=4, separators=(',', ': '),ensure_ascii=False)) #.replace('\\"','\"')
    else:
        tasks.write(json.dumps(final_results,ensure_ascii=False))
    tasks.close()
extract_final_results(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])

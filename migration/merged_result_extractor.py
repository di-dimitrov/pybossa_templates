import sys
import json
import psycopg2
from googletrans import Translator
translator = Translator()

#Only use when annotation for a given task has been finished!!!
def extractEditorDbResults(project_id,phase,fromm,to,user_id=-1,latest_X_anno=1):
                connection = psycopg2.connect(user="root",password="1234",host="127.0.0.1",port="5432",database="pybossa")
                #print('{}, {}, {}'.format(project_id,user_id,latest_X_anno)); print(connection);
                try:
                                cursor = connection.cursor()
                                final_results = []; #print(user_id == -1); print(user_id); print(type(int(user_id)))
                                if(int(user_id) == -1):
                                                query = "select task_id,string_agg(info::text,'##'),string_agg(task_info::text, '##') from (select tr.task_id,tr.user_id, rank () OVER (PARTITION BY tr.task_id ORDER BY tr.finish_time DESC) as rank,tr.info,t.info as task_info from task t join task_run tr on tr.task_id = t.id where tr.project_id = %s order by tr.finish_time,tr.task_id) ranked where rank <= %s and task_id >= %s and task_id <= %s group by task_id"
                                                cursor.execute(query,(project_id,latest_X_anno,fromm,to)); print(cursor)
                                                results = cursor.fetchall()
                                                #print(results)
                                                for row in results:
                                                                row_result = convertDatabaseResultToAnnotationTask(row,latest_X_anno,phase)
                                                                if not len(row_result) == 0:
                                                                                final_results.append(row_result)
                                                #TODO Logic for consolidation on edited text
                                                #TODO For consolidation logic create array of jsons with each json containing the task_run + the user's name for each entry/annotation
                                else:
                                                query = "select task_id,string_agg(info::text ,'##') from (select tr.task_id,tr.user_id, rank () OVER (PARTITION BY tr.task_id ORDER BY tr.finish_time DESC) as rank,tr.info from task t join task_run tr on tr.task_id = t.id where tr.project_id = %s and tr.user_id = %s order by tr.finish_time,tr.task_id) ranked where rank <= %s group by task_id;"
                                                cursor.execute(query,(project_id,user_id,latest_X_anno))
                                                results = cursor.fetchall()
                                                #print(results)
                                                for row in results:
                                                                row_result = convertDatabaseResultToAnnotationTask(row,latest_X_anno,phase)
                                                                if not len(row_result) == 0:
                                                                                final_results.append(row_result)
                                return final_results
                except (Exception, psycopg2.Error) as error:
                                raise("Error fetching data from PostgreSQL table", error)

                finally:
                                # closing database connection
                                if (connection):
                                                cursor.close()
                                                connection.close()
                                                print("PostgreSQL connection is closed \n")

def convertDatabaseResultToAnnotationTask(row, latest_X_anno,phase):
                task_run_values = row[1].split('##')
                task_values = row[2].split('##')
                temp = {}
                temp['answers'] = {}
                current_range = len(task_values)
                translator = Translator()
                for i in range(current_range):
                                task_run_info = json.loads(task_run_values[i])
                                task_info = json.loads(task_values[i])
                                if(phase == 'anno'):
                                                temp.pop('answers',None)
                                                if(not task_run_info['isSkipped']):
                                                                if 'link' in task_run_info:
                                                                        temp['link'] = task_run_info['link']
                                                                temp[i] = dict([('text',task_run_info['text']),('image',task_run_info['image'])])
                                elif(phase == 'cons'):
                                                #print(task_info)
                                                temp['text'] = task_info['0']['text']
                                                temp['image'] = task_info['0']['image']
                                                if 'link' in task_info:
                                                    temp['link'] = task_info['link']
                                                temp['answers'].update(dict([(i,task_run_info)]))
                                elif(phase == 'mk_translate'):
                                                temp.pop('answers',None)
                                                if(not task_run_info['isSkipped']):
                                                                temp['translation'] = translator.translate(task_run_info['text'],src='mk').text
                                                                if 'link' in task_run_info:
                                                                        temp['link'] = task_run_info['link']
                                                                temp[i] = dict([('text',task_run_info['text']),('image',task_run_info['image'])])
                                #elif(phase == 'image_anno'):
                                #                temp.pop('answers',None)
                                #                temp['text'] = task_info['text']
                                #                temp['image'] = task_info['image']
                                #                if 'notes' in task_run_info:
                                #                        temp['notes'] = task_run_info['notes']
                                #                temp.update(dict([(i,task_run_info)]))
                                #elif(phase == 'image_cons'):
                                #                temp['text'] = task_info['text']
                                #                temp['image'] = task_info['image']
                                #                temp['answers'].update(dict([(i,task_run_info)]))
                return temp

def save_tasks(final_results, filename):
                tasks = open(filename,"w")#sys.argv[1] => final file name
                tasks.write(json.dumps(final_results))
                tasks.close()

def updateTaskJsonWithEditedJson(project_id, before_editing):
                initial_results = extractEditorDbResults(project_id)
                old_tasks_file = open(before_editing,"r")
                old_tasks = json.load(old_tasks_file)
                for old_task in old_tasks:
                                for edited_task in initial_results:
                                                if(edited_task['image'] == old_task['image']):
                                                                old_task['text'] = edited_task['text']
                old_tasks_file.close()
                save_tasks(old_tasks,before_editing + '_reedit')

if __name__ == "__main__":
                #sys.argv[1]=> filename for end result
                #sys.argv[2]=> project_id
                #sys.argv[3]=> user_id
                #sys.argv[4]=> latest X annotations
                #sys.argv[5]=> phase: text_anno,text_cons,image_anno,image_cons
                result = extractEditorDbResults(sys.argv[2],sys.argv[5],sys.argv[6],sys.argv[7],int(sys.argv[3]), int(sys.argv[4])); #print('{},{},{},{},{}'.format(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5]))
                #print(result)
                #updateTaskJsonWithEditedJson(sys.argv[2],sys.argv[4])# sys.argv[4]=> json with pre-editing tasks
                save_tasks(result,sys.argv[1])

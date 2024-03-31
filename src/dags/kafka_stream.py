import json
import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime




# default_args = {
#     'owner': 'dvthanh',
#     # 'start_date': datetime(2024, 3, 1, 10, 00), # 10:00, 01/03/2024
#     'start_date': days_ago(0),
#     'retries': 1
# }




def get_data(link):
    res = requests.get(link)
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=3))
    return res


def format_data(res):
    data = {}
    location = res['location']

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}" \
                          + f"{location['city']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data(link):
    res = get_data(link)
    res = format_data(res)
    print(json.dumps(res, indent=3))
    
    

# dag = DAG(
#     'user_automation',
#     default_args = default_args,
#     schedule_interval = '@daily',
#     catchup = False
# )
#
# streaming_task = PythonOperator(
#     task_id = 'stream_data_from_api',
#     python_callable = stream_data,
#     dag = dag
# )



api_link = 'https://randomuser.me/api/'
stream_data(api_link)



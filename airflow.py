import streamlit as st
from datetime import datetime, timedelta
import ast
import json

st.set_page_config(page_title="Airflow Automation Portal", layout="wide")
st.markdown("""
<style>
.block-container { max-width: 950px !important; margin: auto;}
.stExpander { padding-bottom: 18px; }
.section-title { font-size: 20px; font-weight: 600; margin-top: 25px; margin-bottom: 8px;}
.stTextInput>div>div>input, .stNumberInput>div>input, .stTextArea textarea, .stDateInput input { background: #f3f5f7; }
.stTextInput>label, .stNumberInput>label, .stCheckbox>label { color: #555; font-size: 15px; }
</style>
""", unsafe_allow_html=True)
st.markdown("<h1 style='text-align: center; margin-bottom:30px;'>Airflow Automation Portal</h1>", unsafe_allow_html=True)

def test_syntax(code):
    try:
        ast.parse(code)
        return True, "Syntax check passed! DAG code is valid Python."
    except SyntaxError as e:
        return False, f"Syntax error: {e}"

automation_mode = st.selectbox(
    "What do you want to automate?",
    ["Select...", "Airflow Connection Management", "Orchestrate a Data Pipeline"]
)

default_end_date = datetime(2099, 12, 31)

if automation_mode == "Airflow Connection Management":
    st.markdown('<div class="section-title">Provision a Connection DAG</div>', unsafe_allow_html=True)
    tech_choice = st.selectbox(
        "Connection Technology",
        ["Snowflake", "Oracle", "Informatica", "Postgres", "Other"]
    )
    dag_col1, dag_col2 = st.columns(2)
    with dag_col1:
        dag_name = st.text_input("Provisioning DAG Name (ID)", value="add_connection_dag")
        schedule = st.text_input("Schedule Interval", value="@once")
        max_active_runs = st.number_input("Max Active Runs", min_value=1, max_value=1, value=1)
        catchup = st.checkbox("Catchup", value=False)
    with dag_col2:
        description = st.text_area("DAG Purpose", "", height=70)
        tags = st.text_input("Tags (for audit/monitoring)", value="")
        enable_dag_timeout = st.checkbox("Enable DAG Run Timeout", value=False)
        dag_timeout_value, dag_timeout_unit = 0, "minutes"
        if enable_dag_timeout:
            tcol1, tcol2 = st.columns([2, 1])
            with tcol1:
                dag_timeout_value = st.number_input("Run Timeout Value", min_value=1, max_value=1440, value=30)
            with tcol2:
                dag_timeout_unit = st.selectbox("Timeout Unit", options=["minutes", "seconds", "hours"], index=0)
    dagrun_timeout = {
        "enabled": enable_dag_timeout,
        "value": dag_timeout_value,
        "unit": dag_timeout_unit,
    }

    if tech_choice == "Snowflake":
        conn_id = st.text_input("Connection Name (ID)", value="my_snowflake_conn")
        host = st.text_input("Host", value="", help="Snowflake host, e.g. xyz.region.snowflakecomputing.com")
        port = st.text_input("Port", value="", help="Snowflake port, e.g. 443")
        schema = st.text_input("Schema", value="")
        account = st.text_input("Account", value="")
        region = st.text_input("Region", value="")
        database = st.text_input("Database Name", value="")
        role = st.text_input("Role", value="")
        warehouse = st.text_input("Warehouse", value="")
        login = ''
        password = ''
        informatica_domain = ''
        informatica_service = ''
    else:
        conn_id = st.text_input("Connection Name (ID)", value="my_conn")
        host = st.text_input("Host/Account", value="", help=f"{tech_choice} host/server/account")
        port = st.text_input("Port", value="", help="DB port (if any)")
        login = st.text_input("User/Account", value="")
        password = st.text_input("Password", value="", type="password")
        schema = st.text_input("Schema", value="")
        database = st.text_input("Database Name", value="")
        role = st.text_input("Role", value="") if tech_choice == "Oracle" else ""
        warehouse = st.text_input("Warehouse", value="") if tech_choice == "Postgres" else ""
        account = st.text_input("Account", value="") if tech_choice == "Other" else ""
        region = st.text_input("Region", value="") if tech_choice == "Other" else ""
        informatica_domain = st.text_input("Informatica Domain", value="") if tech_choice == "Informatica" else ""
        informatica_service = st.text_input("Informatica Service", value="") if tech_choice == "Informatica" else ""

    op_kwargs = {
        "conn_id": conn_id,
        "conn_type": tech_choice.lower(),
        "host": host,
        "port": port,
        "login": login,
        "password": password,
        "schema": schema,
        "database": database,
        "role": role,
        "warehouse": warehouse,
        "account": account,
        "region": region,
        "informatica_domain": informatica_domain,
        "informatica_service": informatica_service,
    }
    op_kwargs_str = ",\n            ".join([f'"{k}": "{v}"' for k, v in op_kwargs.items() if v])

    imports_section = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.settings import Session
from datetime import datetime, timedelta
import boto3
import json
import os
'''

    add_conn_callable_code = '''
def add_connection_callable(
    conn_id, conn_type, host='', port='', schema='', account='', region='', database='', role='', warehouse='',
    login='', password='', informatica_domain='', informatica_service='', **kwargs
):
    import boto3, json, os

    if conn_type == "snowflake":
        team_identifier = os.environ["AIRFLOW__MWAA__TEAM__NAME"]
        region_name = 'eu-west-1'
        secrets_prefix = f"airflow/connections/{team_identifier}"
        secrets_client = boto3.session.Session().client(service_name='secretsmanager', region_name=region_name)
        response = secrets_client.list_secrets(Filters=[{"key":"name", "values":[secrets_prefix]}])
        variable_string = response['SecretList'][0]['Name']
        secret_value = secrets_client.get_secret_value(SecretId=variable_string )
        secrets = json.loads(secret_value['SecretString'])
        sf_user = secrets.get('user')
        sf_password = secrets.get('password')
        login = sf_user
        password = sf_password

    session = Session()
    db_conn = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()

    extra_fields = {}
    if conn_type == "snowflake":
        extra_fields.update({
            "account": account, "region": region, "schema": schema, "database": database,
            "role": role, "warehouse": warehouse
        })
    elif conn_type == "oracle":
        extra_fields.update({
            "service_name": database, "schema": schema, "role": role
        })
    elif conn_type == "informatica":
        extra_fields.update({
            "domain": informatica_domain, "service": informatica_service
        })
    else:
        extra_fields.update({"database": database, "schema": schema})
    extra_json = json.dumps({k: v for k, v in extra_fields.items() if v})

    if db_conn is None:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            port=port,
            login=login,
            password=password,
            extra=extra_json,
            schema=schema if schema else None
        )
        session.add(new_conn)
        session.commit()
        print(f"Connection created: {conn_id}")
    else:
        needs_update = False
        if db_conn.password != password:
            db_conn.password = password
            db_conn.login = login
            needs_update = True
        if db_conn.host != host or db_conn.port != port or db_conn.extra != extra_json:
            db_conn.host = host
            db_conn.port = port
            db_conn.extra = extra_json
            needs_update = True
        if needs_update:
            session.commit()
            print(f"Connection updated: {conn_id}")
        else:
            print(f"Connection {conn_id} already up to date.")
    session.close()
'''

    default_args_code = (
        "default_args = {\n"
        "    'owner': 'airflow_user',\n"
        f"    'start_date': datetime({datetime.now().year}, {datetime.now().month}, {datetime.now().day}),\n"
        "    'retries': 1,\n"
        "    'retry_delay': timedelta(minutes=5),\n"
        "}\n"
    )
    dag_args = f"dag_id='{dag_name}',\n    description='{description}',\n    schedule_interval='{schedule}',\n    catchup={catchup},\n    max_active_runs={max_active_runs},\n"
    if tags:
        tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        dag_args += f"    tags={tags_list},\n"
    if dagrun_timeout["enabled"] and dagrun_timeout["value"] > 0:
        dag_args += f"    dagrun_timeout=timedelta({dagrun_timeout['unit']}={dagrun_timeout['value']}),\n"
    dag_args += "    default_args=default_args"
    dag_header = f"\nwith DAG(\n{dag_args}\n):"
    task_code = f'''
    add_connection = PythonOperator(
        task_id="add_connection",
        python_callable=add_connection_callable,
        op_kwargs={{\n            {op_kwargs_str}\n        }},
        dag=dag
    )
    '''
    dag_code = (
        f"{imports_section}\n"
        f"{add_conn_callable_code}\n"
        f"{default_args_code}\n"
        f"{dag_header}\n"
        f"{task_code}"
    )

    st.code(dag_code, language="python")
    syntax_valid, syntax_message = test_syntax(dag_code)
    if syntax_valid:
        st.success(syntax_message)
        st.download_button(
            label="Download Connection DAG .py file",
            data=dag_code,
            file_name=f"{dag_name}.py",
            mime="text/x-python",
        )
    else:
        st.error(syntax_message)

elif automation_mode == "Orchestrate a Data Pipeline":
    st.markdown('<div class="section-title">Pipeline DAG Orchestration</div>', unsafe_allow_html=True)

    dag_col1, dag_col2 = st.columns(2)
    with dag_col1:
        dag_name = st.text_input("DAG Name (ID)", value="data_pipeline_dag", help="Unique pipeline name.")
        schedule = st.text_input("Schedule Interval", value="@daily", help="Cron, e.g. '@daily', '0 6 * * *'.")
        max_active_runs = st.number_input("Max Active Runs", min_value=1, max_value=20, value=1)
        catchup = st.checkbox("Catchup", value=False)
    with dag_col2:
        description = st.text_area("Purpose/Business Description", "", height=70)
        tags = st.text_input("Tags for Monitoring", value="")
        enable_dag_timeout = st.checkbox("Enable DAG Run Timeout", value=False)
        dag_timeout_value, dag_timeout_unit = 0, "minutes"
        if enable_dag_timeout:
            tcol1, tcol2 = st.columns([2, 1])
            with tcol1:
                dag_timeout_value = st.number_input("Pipeline Timeout Value", min_value=1, max_value=1440, value=60)
            with tcol2:
                dag_timeout_unit = st.selectbox("Timeout Unit", options=["minutes", "seconds", "hours"], index=0)
    dagrun_timeout = {
        "enabled": enable_dag_timeout,
        "value": dag_timeout_value,
        "unit": dag_timeout_unit,
    }
    with st.expander("Default Args (Task Settings)", expanded=True):
        st.markdown("#### Essentials", unsafe_allow_html=True)
        col_owner, col_start, col_end = st.columns([1, 1, 1])
        with col_owner:
            owner = st.text_input("Owner", value="airflow_user", help="Person or team responsible.")
        with col_start:
            start_date = st.date_input("Start Date", datetime.today())
        with col_end:
            end_date = st.date_input("End Date", default_end_date)
        st.markdown("---")
        col_dep, col_callback = st.columns([1, 2])
        with col_dep:
            depends_on_past = st.checkbox("Depends on previous run", value=False)
        with col_callback:
            on_failure_callback = st.text_input("On Failure Callback function", value="", help="Alerting/email function.")
        st.markdown("#### Retry Settings", unsafe_allow_html=True)
        col_retry, col_delay, col_unit = st.columns([1, 1, 1])
        dag_retries_used = col_retry.checkbox("Set default DAG retries (all tasks)", value=False)
        if dag_retries_used:
            dag_retries = col_delay.number_input("DAG Retry count", min_value=0, max_value=10, value=1)
            delay_value = col_delay.number_input("Retry Delay", min_value=1, max_value=3600, value=5)
            delay_unit = col_unit.selectbox("Delay Unit", options=["minutes", "seconds"], index=0)
            if delay_unit == "minutes":
                retry_delay_expr = f"timedelta(minutes={delay_value})"
            else:
                retry_delay_expr = f"timedelta(seconds={delay_value})"
        else:
            dag_retries, retry_delay_expr = None, None

    default_args = {
        "owner": owner,
        "start_date": start_date,
        "depends_on_past": depends_on_past,
    }
    if end_date != default_end_date:
        default_args["end_date"] = end_date
    if on_failure_callback:
        default_args["on_failure_callback"] = on_failure_callback
    if dag_retries is not None:
        default_args["retries"] = dag_retries
        default_args["retry_delay"] = retry_delay_expr

    st.markdown("---")
    st.header("Tasks")
    num_tasks = st.number_input("Number of Tasks", min_value=1, max_value=20, value=1, step=1)

    tasks = {}
    task_retries = {}
    task_operator_types = {}
    task_sqls = {}
    task_conn_ids = {}
    task_callables = {}
    task_provide_context = {}
    task_op_kwargs = {}

    for i in range(num_tasks):
        with st.container():
            st.markdown(f"**Task {i+1}**", unsafe_allow_html=True)
            tcol1, tcol2, tcol3 = st.columns([1, 2, 5])
            task_id = tcol1.text_input(f"Task ID {i+1}", value=f"task{i+1}", key=f"tid_{i}")
            task_name = tcol2.text_input(f"Task Name {i+1}", value=f"task_{i+1}_name", key=f"tname_{i}")
            operator_type = tcol3.selectbox("Operator Type", ["PythonOperator", "SnowflakeOperator"], key=f"op_{i}")
            task_operator_types[task_id] = operator_type

            if operator_type == "PythonOperator":
                python_callable = tcol3.text_input("Python Callable (imported or inline)", key=f"pc_{i}")
                provide_context = tcol3.checkbox("Pass Airflow Context (provide_context)", value=False, key=f"context_{i}")
                op_kwargs_json = tcol3.text_area("op_kwargs (JSON)", value='{}', key=f"opkw_{i}")
                try:
                    op_kwargs_dict = json.loads(op_kwargs_json) if op_kwargs_json else {}
                except Exception:
                    st.warning(f"Invalid op_kwargs JSON for Task {task_id}", icon="⚠️")
                    op_kwargs_dict = {}
                task_callables[task_id] = python_callable.strip()
                task_provide_context[task_id] = provide_context
                task_op_kwargs[task_id] = op_kwargs_json

            if operator_type == "SnowflakeOperator":
                sql = tcol3.text_area("SQL Query", key=f"sql_{i}")
                conn_id = tcol3.text_input("Snowflake Conn ID", "snowflake_default", key=f"conn_{i}")
                task_sqls[task_id] = sql
                task_conn_ids[task_id] = conn_id

            tcol_retry, tcol_ovr = st.columns([2,2])
            task_custom_retry = tcol_ovr.checkbox("Set custom retries for this task", key=f"tr_use_{i}")
            if task_custom_retry:
                retries_this_task = tcol_retry.number_input(f"Retries for {task_id}", min_value=0, max_value=10, value=1, key=f"tr_{i}")
                task_retries[task_id] = retries_this_task
            else:
                task_retries[task_id] = None

            tasks[task_id] = task_name

    st.markdown("---")
    st.header("Set Dependencies")
    dependencies = {}
    for i, upstream in enumerate(tasks.keys()):
        with st.expander(f"Task: {upstream} dependencies", expanded=False):
            downstream_list = []
            for downstream in tasks.keys():
                if downstream != upstream:
                    if st.checkbox(f"{upstream} → {downstream}", key=f"dep_{upstream}_{downstream}"):
                        downstream_list.append(downstream)
            if downstream_list:
                dependencies[upstream] = downstream_list

    st.markdown("---")
    st.write("")

    def generate_dag_code(
        dag_name, schedule, catchup, max_active_runs, description, tags, dagrun_timeout,
        default_args, tasks, dependencies, task_retries, task_operator_types, task_sqls, task_conn_ids, task_callables, task_provide_context, task_op_kwargs):
        imports_section = '''
from airflow import DAG
from datetime import datetime, timedelta
'''
        op_types = set(task_operator_types.values())
        if "PythonOperator" in op_types:
            imports_section += "from airflow.operators.python import PythonOperator\n"
        if "SnowflakeOperator" in op_types:
            imports_section += "from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator\n"

        tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        tags_list_str = "[" + ", ".join(f'"{tag}"' for tag in tags_list) + "]" if tags_list else None

        default_args_code = "default_args = {\n"
        for k, v in default_args.items():
            if k in ("start_date", "end_date"):
                if v:
                    default_args_code += f"    '{k}': datetime({v.year}, {v.month}, {v.day}),\n"
            elif isinstance(v, str) and v.startswith("timedelta"):
                default_args_code += f"    '{k}': {v},\n"
            elif isinstance(v, str):
                default_args_code += f"    '{k}': '{v}',\n"
            elif v is not None:
                default_args_code += f"    '{k}': {v},\n"
        default_args_code += "}\n"

        dag_args_part = f"dag_id=\"{dag_name}\",\n"
        dag_args_part += f"    schedule_interval=\"{schedule}\",\n"
        dag_args_part += f"    catchup={catchup},\n"
        dag_args_part += f"    max_active_runs={max_active_runs},\n"
        if description:
            dag_args_part += f"    description=\"{description}\",\n"
        if tags_list_str:
            dag_args_part += f"    tags={tags_list_str},\n"
        if dagrun_timeout["enabled"] and dagrun_timeout["value"] > 0:
            dag_args_part += f"    dagrun_timeout=timedelta({dagrun_timeout['unit']}={dagrun_timeout['value']}),\n"
        dag_args_part += "    default_args=default_args"
        pipeline_args = f"\nwith DAG(\n{dag_args_part}\n):"

        tasks_code = ""
        for task_id, task_name in tasks.items():
            operator_type = task_operator_types[task_id]
            retries_code = ""
            if task_id in task_retries and task_retries[task_id] is not None:
                retries_code = f", retries={task_retries[task_id]}"
            if operator_type == "PythonOperator":
                py_callable = task_callables.get(task_id, "lambda: None")
                provide_context = task_provide_context.get(task_id, False)
                op_kwargs_json = task_op_kwargs.get(task_id, "{}") or "{}"
                tasks_code += f'''
    {task_id} = PythonOperator(
        task_id="{task_name}",
        python_callable={py_callable},
        provide_context={provide_context},
        op_kwargs={op_kwargs_json},
        default_args=default_args{retries_code}
    )
'''
            elif operator_type == "SnowflakeOperator":
                sql = task_sqls.get(task_id, "")
                conn_id = task_conn_ids.get(task_id, "snowflake_default")
                tasks_code += f'''
    {task_id} = SnowflakeOperator(
        task_id="{task_name}",
        sql=\"\"\"{sql}\"\"\",
        snowflake_conn_id="{conn_id}",
        default_args=default_args{retries_code}
    )
'''

        dependencies_code = ""
        for upstream, downstream_list in dependencies.items():
            for downstream in downstream_list:
                dependencies_code += f"    {upstream} >> {downstream}\n"

        dag_code = f'''
{imports_section}
{default_args_code}
{pipeline_args}
{tasks_code}
{dependencies_code}
'''
        return dag_code

    if st.button("Generate DAG Code"):
        dag_code = generate_dag_code(
            dag_name, schedule, catchup, max_active_runs, description, tags, dagrun_timeout, default_args,
            tasks, dependencies, task_retries,
            task_operator_types, task_sqls, task_conn_ids, task_callables, task_provide_context, task_op_kwargs
        )
        syntax_valid, syntax_message = test_syntax(dag_code)
        st.code(dag_code, language="python")
        if syntax_valid:
            st.success(syntax_message)
            st.download_button(
                label="Download DAG .py file",
                data=dag_code,
                file_name=f"{dag_name}.py",
                mime="text/x-python",
            )
        else:
            st.error(syntax_message)

else:
    st.info("Choose an automation target above to begin workflow configuration.")

import panel as pn
import subprocess, time, sys
import datetime as dt
from datetime import datetime
from pathlib import Path
import json
import jsonschema_default
import param
import pandas as pd, numpy as np
import asyncio, io
import pydantic
from task_common import send, receive, receive_df

pn.extension('ace', 'jsoneditor', "terminal", 'floatpanel', "tabulator", css_files=["https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css"])
Path("/tmp/panel_script_launcher_schema").mkdir(exist_ok=True)
Path("/tmp/panel_script_launcher_args").mkdir(exist_ok=True)
script_folder = Path("/media/filer2/T4b/SharedCode/RunnerScripts/")

session = "session_"+str(datetime.now())
filter_expr = None

run_df = pn.rx(pd.DataFrame())
conn = []

async def update_run_df():
    try:
        p = await asyncio.subprocess.create_subprocess_exec("python", "task_query.py", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        prev_str = ""
        while True:
            request = dict(action="get_task_df") #filter_expr=f"session='{session}'"
            r_str = (json.dumps(request)+"\n").encode()
            p.stdin.write((str(len(r_str))+"\n").encode())
            p.stdin.write(r_str)
            await p.stdin.drain()
            size_b = await p.stdout.readline()
            size_s = size_b.decode()
            ret_size = int(size_s)
            ret = (await p.stdout.read(ret_size)).decode()
            if ret != prev_str:
                print("updating")
                table = pd.read_json(io.StringIO(ret), orient="table")
                run_df.rx.value = table
                prev_str = ret
            await asyncio.sleep(2)
    except: raise
    finally:
        p.terminate()

pn.state.onload(update_run_df)

update_btn = pn.widgets.Button(name="⟳", align=("start", "end"), margin=0)
scripts_dict = pn.rx(lambda c: {str(p.relative_to(script_folder)):p for p in script_folder.glob("**/*.py")})(update_btn)
script_selector = pn.widgets.Select(name='Select Script', options=pn.rx(lambda d: list(d.keys()))(scripts_dict), margin=(0, 0, 0, 10))
has_schema_error = pn.rx(False)
args_read_value = None

def get_schema(script, d):
    try:
        schema_file = Path(f"/tmp/panel_script_launcher_schema/{datetime.now()}.json")
        subprocess.run(["python", str(d[script]), "--export_schema", "--schema_file", str(schema_file)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with schema_file.open("r") as f:
            schema = json.load(f)
        schema_file.unlink()
    except Exception as e:
        has_schema_error.rx.value= True
        print(e)
        return dict(properties=dict(args=dict(type="string", default=f"Error while loading schema for arguments {e}")))
    has_schema_error.rx.value = False
    return schema

# schema = pn.rx(get_schema)(script_selector, scripts_dict)
args_selector = pn.pane.Placeholder()

def rm_path_format(schema):
    if isinstance(schema, dict):
        if "format" in schema and schema["format"]=="path":
            return {k:rm_path_format(v) for k, v in schema.items() if not k=="format"}
        else:
            return {k:rm_path_format(v) for k, v in schema.items()}
    elif isinstance(schema, list):
        return [rm_path_format(v) for v in schema]
    else:
        return schema

def args_selector_update(script):
    global args_read_value
    if not script in scripts_dict.rx.value:
        args_selector.update(pn.pane.Alert("Could not find script"))
    else:
        schema = get_schema(script, scripts_dict.rx.value)
        if has_schema_error.rx.value:
            args_selector.update(pn.pane.Alert("Error retrieving script information"))
        else:
            if args_read_value is None:
                args_selector.update(pn.widgets.JSONEditor(value=jsonschema_default.create_from(schema), schema=rm_path_format(schema), menu=False, search=False, width_policy='max'))
            else:
                args_selector.update(pn.widgets.JSONEditor(value=args_read_value, schema=rm_path_format(schema), menu=False, search=False, width_policy='max'))
                args_read_value=None
args_selector_update(script_selector.value)
pn.bind(args_selector_update, script_selector, watch=True)

load_run_btn = pn.widgets.FileInput(name="Load run parameters", accept=".yaml")
save_run_btn = pn.widgets.Button(name="Save run parameters", disabled=has_schema_error)

download_btn = pn.widgets.FileDownload(file="error", visible=False)
# upload_widget = pn.widgets.FileInput(multiple=False)

def download_run_params_file():
    d = dict(script=script_selector.value, args=args_selector.object.value)
    from io import StringIO
    import yaml
    f = StringIO(yaml.dump(d))
    download_btn.file = f
    download_btn.filename = f'run_parameters--{script_selector.value}--{datetime.now():%Y-%m-%d}.yaml'
    download_btn._clicks+=1

save_run_btn.on_click(lambda ev: download_run_params_file())
def load_params_from_file(data):
    global args_read_value
    import yaml
    params = yaml.safe_load(data.decode())
    args_read_value = params["args"]
    if params["script"] != script_selector.value:
        script_selector.value = params["script"]
    else:
        args_selector_update(params["script"])
    if isinstance(args_selector.object, pn.widgets.JSONEditor):
        pn.bind(lambda v: load_run_btn.clear(), args_selector.object, watch=True)

pn.bind(load_params_from_file, load_run_btn, watch=True)


task_name = pn.widgets.TextInput(placeholder="Unique name of task")
import re
# is_invalid_task_name = pn.rx(lambda s, df: re.fullmatch("\w+", s) is None or s in df["task_name"].to_list())(task_name.param.value_input, my_runs)
run_btn = pn.widgets.Button(name="Add Task")

async def add_run(e):
    try:
        p = await asyncio.subprocess.create_subprocess_exec("python", "task_query.py", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        request = dict(action="run_task", run_type="python", conda_env="dbscripts",
                script=str(scripts_dict.rx.value[script_selector.value].resolve()), 
                args=args_selector.object.value,
                creation_date = str(dt.datetime.now(tz=dt.timezone.utc).astimezone())
        )
        r_str = (json.dumps(request)+"\n").encode()
        p.stdin.write((str(len(r_str))+"\n").encode())
        p.stdin.write(r_str)
        await p.stdin.drain()
        size_b = await p.stdout.readline()
        size_s = size_b.decode()
        ret_size = int(size_s)
        id = (await p.stdout.read(ret_size)).decode()
        p.terminate()
    except: raise
    finally:
        p.terminate()

 #disabled=has_schema_error.rx.or_(is_invalid_task_name)

# def add_run():
#     new_df = pd.concat([my_runs.rx.value, pd.DataFrame([dict(task_name=task_name.value, script=script_selector.value, add_date=datetime.now(), status="Queued")])], ignore_index=True)
#     my_runs.rx.value = new_df

run_btn.on_click(add_run)


new_run = pn.Card(load_run_btn, pn.Row(script_selector, update_btn), args_selector, pn.Row(task_name, save_run_btn, download_btn, run_btn), title="New Run", width_policy='max')

run_table= pn.widgets.Tabulator(run_df, width_policy='max', selectable='checkbox', hidden_columns=["index"], sorters=[dict(field="id", dir="desc")])
# header_filters=dict(Queued={'type': 'input', 'func': 'like', 'placeholder': 'filter'})

exec_btn = pn.widgets.Button(name="Execute Tasks")
cancel_btn = pn.widgets.Button(name="Cancel Tasks")
remove_btn = pn.widgets.Button(name="Remove Tasks")

no_runs = pn.pane.Alert("No tasks to display")
tasks = pn.rx(lambda d:  pn.Column(run_table, pn.Row(exec_btn, cancel_btn, remove_btn)) if len(d.index) >0 else no_runs)(run_df)
run_manager = pn.Card(tasks, title="Task Manager", width_policy='max')


task_viewer = pn.Card(title="Task Viewer", width_policy='max')



pn.Column(new_run, pn.Spacer(height=20), run_manager, pn.Spacer(height=20), task_viewer, width_policy='max').servable()




# default_args_values = {}


# args = ...



# args = pn.widgets.JSONEditor(width_policy='max', menu=False, search=False, schema=rm_path_format(schema.rx.value), value=pn.rx(get_args_value)(script_selector, schema))

# save_args = pn.widgets.Button(name="Save script arguments", disabled=has_schema_error)
# load_args = pn.widgets.Button(name="Load script arguments", disabled=has_schema_error)
# run_btn = pn.widgets.Button(name="Run script", disabled=has_schema_error)
# import yaml
# def assign(v):
#     print(f"TOTO\n\n{yaml.dump(v)}\n\n")
#     args.schema = v
#     print("done")
#     print(f"TOTO\n\n{yaml.dump(args.schema)}\n\n")
# pn.bind(assign, panel_schema, watch=True)
# # pn.bind(lambda v: print(f"TOTO\n\n{yaml.dump(v)}\n\n"), panel_schema, watch=True)
# pn.Column(pn.Row(script_selector, update_btn), pn.Row(load_args, save_args), args, run_btn).servable()






# files = dict(test1="/media/filer2/T4b/UserFolders/Julien/ArgInterface/test_pydantic_cli.py", test2="/media/filer2/T4b/UserFolders/Julien/ArgInterface/test_pydantic_cli.py")
# args = {k: None for k in files.keys()}

# select = pn.widgets.Select(name='Select Script', options=list(files.keys()))

# editor = pn.widgets.JSONEditor(width_policy='max')
# current_file = select.value
# run_btn = pn.widgets.Button(name="Run script")

# result_selector = pn.widgets.Select(name="Result Selector", options=["stdout", "stderr", "stdout+stderr", "input_params", "meta_information"], visible=False)
# result_placeholder =pn.pane.Placeholder()

# def update_display(v):
#     global current_file
#     if not args[current_file] is None:
#         args[current_file] = editor.value
#     f = files[v]
#     if args[v] is None:
#         schema_file = Path(f"/tmp/panel_script_launcher_schema/{datetime.now()}.json")
#         subprocess.run(["python", str(f), "--export_schema", "--schema_file", str(schema_file)], check=True)
#         with schema_file.open("r") as f:
#             schema = json.load(f)
#         schema_file.unlink()
#         args[v] = jsonschema_default.create_from(schema)
#     editor.value = args[v]
#     current_file = v
#     def rm_path_format(schema):
#         if isinstance(schema, dict):
#             if "format" in schema and schema["format"]=="path":
#                 return {k:rm_path_format(v) for k, v in schema.items() if not k=="format"}
#             else:
#                 return {k:rm_path_format(v) for k, v in schema.items()}
#         elif isinstance(schema, list):
#             return [rm_path_format(v) for v in schema]
#         else:
#             return schema
#     editor.schema = rm_path_format(schema)

# def run_script(c):
#     import selectors
#     errors = pn.widgets.Terminal(write_to_console=False,width_policy='max', options={"cursorBlink": True})
#     result_placeholder.update(errors)
#     f = files[current_file]
#     args_file = Path(f"/tmp/panel_script_launcher_args/{datetime.now()}.json")
#     with args_file.open("w") as af:
#         json.dump(editor.value, af)
#     p = subprocess.Popen(["python", str(f), "--config_file", str(args_file)], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
#     sel = selectors.DefaultSelector()
#     sel.register(p.stdout, selectors.EVENT_READ)
#     sel.register(p.stderr, selectors.EVENT_READ)
#     ok = True
#     while ok:
#         for key, val1 in sel.select():
#             line = key.fileobj.readline()
#             if not line:
#                 ok = False
#                 break
#             if key.fileobj is p.stdout:
#                 errors.write(line)
#             else:
#                errors.write(line)
        
#     errors.write(f'Process finished with status {p.returncode}')

# update_display(select.value)
# pn.bind(update_display, select, watch=True)
# run_btn.on_click(run_script)

# pn.Column(select, editor, run_btn, result_selector, result_placeholder,width_policy='max').servable()
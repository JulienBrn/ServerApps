import asyncio, json, yaml
from task_common import send, receive, send_df, receive_df
import datetime as dt
from pathlib import Path
import pandas as pd
import os


task_save_folder = Path(os.path.expandvars("$HOME/.tasks"))
task_save_folder.mkdir(exist_ok=True)
task_index_tsv = task_save_folder/"task_index.tsv"
tmp_folder = Path("/tmp/task_runner")
tmp_folder.mkdir(exist_ok=True)

if task_index_tsv.exists():
    load_tasks: pd.DataFrame = pd.read_csv(task_index_tsv, sep="\t")
    task_info_columns = load_tasks.columns.to_list()
    tasks = list(load_tasks.to_dict(orient="records"))
else:
    task_info_columns = []
    tasks = []


tg = asyncio.TaskGroup()
process_limit = asyncio.Semaphore(3)
other_prints = True

def display(*args, **kwargs):
    global other_prints
    print(*args, **kwargs)
    other_prints=True

def save_task(task):
    if "messages" in task:
        i=len(task["messages"]) -1
        while i >=0 and task["messages"][i]["message"]=="":
            task["messages"].pop(i)
            i-=1
    path = task_save_folder/f"task_{task['id']}.yaml"
    with path.open("w") as f:
        yaml.safe_dump(task, f)
    if "messages" in task: del task["messages"]
    if "args" in task: del task["args"]
    task["store_path"] = path
    new_cols = [k for k in task if not k in task_info_columns]
    cols = task_info_columns + new_cols
    with task_index_tsv.open("w") as f:
        pd.DataFrame(tasks)[cols].to_csv(f, index=False, sep="\t", header=True)
    

async def run_task(task):
    global other_prints
    await asyncio.sleep(1)
    task["status"] = "queued"
    try:
        async with process_limit:
            tmp_path = tmp_folder / (str(dt.datetime.now()) + ".yaml")
            with tmp_path.open("w") as f:
                yaml.safe_dump(task["args"], f)
            python_path_p = await asyncio.subprocess.create_subprocess_shell(f"conda run -n {task['conda_env']} which python", stdout=asyncio.subprocess.PIPE)
            python_path, _ = await python_path_p.communicate()

            p = await asyncio.subprocess.create_subprocess_exec(python_path.decode().strip(), task["script"], "--config_file", str(tmp_path), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
            task["messages"] = []
            task["status"] = "running"
            task["__process__"] = p
            finished = False
            try:
                while not finished:
                    async with asyncio.TaskGroup() as rg:
                        out = rg.create_task(p.stdout.readline())
                        err = rg.create_task(p.stderr.readline())
                        end = rg.create_task(p.wait())
                        done, pending = await asyncio.wait([out, err, end], return_when=asyncio.FIRST_COMPLETED)
                        t = dt.datetime.now(tz=dt.timezone.utc).astimezone()
                        for d in done:
                            if d is end:
                                finished=True
                                task["return_code"] = end.result()
                                if tmp_path.exists():
                                    tmp_path.unlink()
                            else:
                                r = d.result().decode()
                                source = "stderr" if d is err else "stdout" if d is out else None
                                task["messages"].append(dict(source=source, date=t, message=r))
            except BaseException as e:
                import os 
                pgid = os.getpgid(p.pid)
                stop = await asyncio.subprocess.create_subprocess_shell(f"pkill --signal 15 -g {pgid}")
                await stop.wait()
                await p.wait()
                raise
            else:
                task["status"] = "success" if task["return_code"] == 0 else "run error"
    except asyncio.CancelledError:
        task["status"] = "Cancelled"
        other_prints=True
    except Exception as e:
        task["status"] = "bug"
        display("BUG")
        display(type(e))
        display(e)
    finally:
        del task["__task__"]
        del task["__process__"]
        save_task(task)
    

async def handle_client(reader: asyncio.StreamReader, writer):
    while True:
        data = await receive(reader)
        if not data:
            break
        elif data["action"] == "run_task":
            tasks.append(data)
            data["id"] = len(tasks)-1
            data["status"] = "unprocessed"
            t = tg.create_task(run_task(data))
            data["__task__"] = t
            await send(writer, dict(id=data["id"]))

        elif data["action"] == "get_task_df":
            df: pd.DataFrame = pd.DataFrame(tasks)[task_info_columns]
            if "filter_expr" in data:
                df = df[df.eval(data["filter_expr"])]
            await send_df(writer, df)
        elif data["action"] == "get_task_info":
            t = tasks[data["id"]]
            if "store_path" in t:
                with Path(t["store_path"]).open("r") as f:
                    t_info = yaml.safe_load(f)
            else:
                t_info = t
            await send(writer, t_info)
        elif data["action"] == "cancel_task":
            t = tasks[data["id"]]
            if "__task__" in t:
                t["status"] = "cancelling"
                t["__task__"].cancel()

        elif data["action"] == "send_stdin":
            t = tasks[data["id"]]
            if "__process__" in t:
                t["__process__"].stdin.write(data["message"].encode())
                await t["__process__"].stdin.drain()
            else:
                display("ignored stdin message")
            
            
async def print_tasks():
    import sys
    global other_prints
    n_display=0
    while True:
        if not other_prints:
            print(f"\033[{n_display}A", end= "")
            print("\033[J", end= "")
        task_df = pd.DataFrame(tasks)
        task_df.drop(columns=["args", "script", "__task__", "messages"], inplace=True, errors="ignore")
        display_str = str(task_df)
        n_display = display_str.count("\n") +1
        sys.stdout.flush()
        print(task_df)
        sys.stdout.flush()
        other_prints=False
        await asyncio.sleep(0.5)

            

async def main():
    async with tg:
        s = await asyncio.start_server(handle_client, host="localhost", port=8888)
        # tg.create_task(print_tasks())
        tg.create_task(s.serve_forever())

asyncio.run(main())
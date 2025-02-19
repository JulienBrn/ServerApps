import asyncio, json
from task_common import send, receive
from pathlib import Path

async def main():
    reader, writer = await asyncio.open_connection("localhost", 8888)
    print(reader)
    print(writer)
    for i in range(1):
    #     Path("test5.mp4").unlink(missing_ok=True)
        d = dict(action="run_task", run_type="python", conda_env="dbscripts",
                script="/media/filer2/T4b/SharedCode/RunnerScripts/video_compression.py", 
                args=dict(input="/media/filer2/T4b/Datasets/Monkey/TrainingFRM/Version-VIDEO-YASMINE/Denver/2025-02-04-vid1/250204_Den_01.mp4", output="test5.mp4"))
        # d = dict(action="run_task", run_type="python", conda_env="dbscripts",
        #         script="/media/filer2/T4b/SharedCode/RunnerScripts/test.py", 
        #         args=dict(time=100))
        await send(writer, d)
        print("sent")
        response = await receive(reader)
        print(response)
        await asyncio.sleep(6)
        print("sending")
        await send(writer, dict(action="send_stdin", id=response["id"], message="y\n"))

        await asyncio.sleep(600)
        await send(writer, dict(action="cancel_task", id=response["id"]))
        



asyncio.run(main())
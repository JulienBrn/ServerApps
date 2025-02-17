import asyncio, json
from task_common import send, receive

async def main():
    reader, writer = await asyncio.open_connection("localhost", 8888)
    d = dict(action="run_task", run_type="python", conda_env="dbscripts",
             script="/media/filer2/T4b/SharedCode/RunnerScripts/video_compression.py", 
             args=dict(input="/media/filer2/T4b/Datasets/Monkey/TrainingFRM/Version-VIDEO-YASMINE/Denver/2025-02-04-vid1/250204_Den_01.mp4", output="test.mp4"))
    
    await send(writer, d)
    print("sent")
    response = await receive(reader)
    print(response)


asyncio.run(main())
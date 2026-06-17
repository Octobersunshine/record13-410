import time
from delay_queue import DelayQueue


def my_callback(name: str, value: int) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] 任务执行: name={name}, value={value}")


def main() -> None:
    print(f"[{time.strftime('%H:%M:%S')}] 延时队列服务启动...")

    with DelayQueue() as queue:
        task1 = queue.submit(1, my_callback, "task1", 100)
        task2 = queue.submit(3, my_callback, "task2", 200)
        task3 = queue.submit(2, my_callback, "task3", 300)

        print(f"[{time.strftime('%H:%M:%S')}] 已提交任务: task1={task1[:8]}, task2={task2[:8]}, task3={task3[:8]}")
        print(f"[{time.strftime('%H:%M:%S')}] 队列长度: {queue.qsize()}")

        queue.cancel(task3)
        print(f"[{time.strftime('%H:%M:%S')}] 取消任务 task3")

        print(f"[{time.strftime('%H:%M:%S')}] 等待任务执行...\n")
        time.sleep(5)

    print(f"\n[{time.strftime('%H:%M:%S')}] 延时队列服务已停止")


if __name__ == "__main__":
    main()

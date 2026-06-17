import os
import time
from delay_queue import DelayQueue

PERSIST_FILE = os.path.join(os.path.dirname(__file__), "tasks.json")


def my_callback(name: str, value: int) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] 任务执行: name={name}, value={value}")


def demo_basic_usage() -> None:
    print("=" * 60)
    print("【示例1】普通任务（不持久化，重启丢失）")
    print("=" * 60)
    print(f"[{time.strftime('%H:%M:%S')}] 延时队列服务启动...")

    with DelayQueue() as queue:
        task1 = queue.submit(1, my_callback, "task1", 100)
        task2 = queue.submit(2, my_callback, "task2", 200)
        print(f"[{time.strftime('%H:%M:%S')}] 已提交任务: task1={task1[:8]}, task2={task2[:8]}")
        print(f"[{time.strftime('%H:%M:%S')}] 等待任务执行...\n")
        time.sleep(4)
    print(f"[{time.strftime('%H:%M:%S')}] 延时队列服务已停止\n")


def demo_persistent_restart() -> None:
    print("=" * 60)
    print("【示例2】持久化任务（模拟服务重启恢复）")
    print("=" * 60)

    if os.path.exists(PERSIST_FILE):
        os.remove(PERSIST_FILE)

    print(f"[{time.strftime('%H:%M:%S')}] 阶段1: 启动服务，提交持久化任务...")
    queue1 = DelayQueue(persist_path=PERSIST_FILE)
    queue1.register_callback("my_callback", my_callback)
    queue1.start()

    task_a = queue1.submit_persistent(30, "my_callback", "task_A", 111)
    task_b = queue1.submit_persistent(5, "my_callback", "task_B", 222)
    task_c = queue1.submit_persistent(8, "my_callback", "task_C", 333)
    print(f"[{time.strftime('%H:%M:%S')}] 已提交持久化任务:")
    print(f"  - task_A (延迟30s, ID={task_a[:8]}...)")
    print(f"  - task_B (延迟5s,  ID={task_b[:8]}...)")
    print(f"  - task_C (延迟8s,  ID={task_c[:8]}...)")

    print(f"[{time.strftime('%H:%M:%S')}] 取消 task_C")
    queue1.cancel(task_c)
    print(f"[{time.strftime('%H:%M:%S')}] 当前队列长度: {queue1.qsize()}")

    print(f"[{time.strftime('%H:%M:%S')}] 等待2秒，模拟服务异常崩溃...\n")
    time.sleep(2)

    print(f"[{time.strftime('%H:%M:%S')}] 停止服务（任务未全部执行完）")
    queue1.stop()
    print(f"[{time.strftime('%H:%M:%S')}] 持久化文件已保存: {PERSIST_FILE}")
    print(f"[{time.strftime('%H:%M:%S')}] ---------- 服务已停止 ----------\n")

    time.sleep(1)

    print(f"[{time.strftime('%H:%M:%S')}] 阶段2: 重新启动服务，从持久化文件恢复任务...")
    queue2 = DelayQueue(persist_path=PERSIST_FILE)
    queue2.register_callback("my_callback", my_callback)
    restored = queue2.load_persistent()
    print(f"[{time.strftime('%H:%M:%S')}] 从磁盘恢复了 {restored} 个任务")
    print(f"[{time.strftime('%H:%M:%S')}] 当前队列长度: {queue2.qsize()}")
    queue2.start()

    print(f"[{time.strftime('%H:%M:%S')}] 等待剩余任务到期执行（注意 task_A 需要约30s后执行）...\n")
    time.sleep(10)

    print(f"\n[{time.strftime('%H:%M:%S')}] 停止服务（剩余任务仍会保存在磁盘）")
    queue2.stop()
    print(f"[{time.strftime('%H:%M:%S')}] 持久化文件保留在: {PERSIST_FILE}")


if __name__ == "__main__":
    demo_basic_usage()
    print()
    demo_persistent_restart()

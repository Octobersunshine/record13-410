import os
import time
from delay_queue import DelayQueue

PERSIST_FILE = os.path.join(os.path.dirname(__file__), "tasks.json")
PERSIST_FILE2 = os.path.join(os.path.dirname(__file__), "tasks_cancel.json")


def my_callback(name: str, value: int) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] 任务执行: name={name}, value={value}")


_executed_tasks: set[str] = set()


def tracking_callback(name: str, value: int) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] ❗ 任务执行: name={name}, value={value}")
    _executed_tasks.add(name)


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


def demo_cancel_feature() -> None:
    print()
    print("=" * 60)
    print("【示例3】任务取消功能（task_id 取消 + 持久化验证）")
    print("=" * 60)

    _executed_tasks.clear()

    if os.path.exists(PERSIST_FILE2):
        os.remove(PERSIST_FILE2)

    print(f"[{time.strftime('%H:%M:%S')}] 启动服务，提交多个任务...")
    queue = DelayQueue(persist_path=PERSIST_FILE2)
    queue.register_callback("tracker", tracking_callback)
    queue.start()

    t_normal_run = queue.submit(3, tracking_callback, "WILL_RUN_1", 1)
    t_cancel_mem = queue.submit(3, tracking_callback, "SHOULD_CANCEL_MEM", 2)
    t_persist_run = queue.submit_persistent(5, "tracker", "WILL_RUN_2", 3)
    t_cancel_persist = queue.submit_persistent(5, "tracker", "SHOULD_CANCEL_DISK", 4)

    print(f"[{time.strftime('%H:%M:%S')}] 已提交 4 个任务，ID 前缀:")
    print(f"  - WILL_RUN_1          (内存, 3s): {t_normal_run[:8]}")
    print(f"  - SHOULD_CANCEL_MEM   (内存, 3s): {t_cancel_mem[:8]}")
    print(f"  - WILL_RUN_2          (持久化,5s): {t_persist_run[:8]}")
    print(f"  - SHOULD_CANCEL_DISK  (持久化,5s): {t_cancel_persist[:8]}")

    print(f"\n[{time.strftime('%H:%M:%S')}] 立即取消 SHOULD_CANCEL_MEM 和 SHOULD_CANCEL_DISK ...")
    r1 = queue.cancel(t_cancel_mem)
    r2 = queue.cancel(t_cancel_persist)
    r3 = queue.cancel("nonexistent_task_id_xyz")
    print(f"  - cancel(SHOULD_CANCEL_MEM)  -> {r1}")
    print(f"  - cancel(SHOULD_CANCEL_DISK) -> {r2}")
    print(f"  - cancel(不存在的ID)         -> {r3}")

    print(f"\n[{time.strftime('%H:%M:%S')}] 等待 7 秒（超过所有任务的到期时间）...\n")
    time.sleep(7)

    print(f"\n[{time.strftime('%H:%M:%S')}] ====== 执行结果验证 ======")
    expected = {"WILL_RUN_1", "WILL_RUN_2"}
    not_expected = {"SHOULD_CANCEL_MEM", "SHOULD_CANCEL_DISK"}
    print(f"实际执行的任务: {sorted(_executed_tasks)}")
    print(f"预期应执行:     {sorted(expected)}")
    print(f"预期不执行:     {sorted(not_expected)}")

    ok_expected = expected.issubset(_executed_tasks)
    ok_notrun = not_expected.isdisjoint(_executed_tasks)
    print(f"\n验证: 应执行的都执行了 -> {'✅ 通过' if ok_expected else '❌ 失败'}")
    print(f"验证: 应取消的都没执行 -> {'✅ 通过' if ok_notrun else '❌ 失败'}")

    queue.stop()

    print(f"\n[{time.strftime('%H:%M:%S')}] 重启服务，验证持久化取消效果...")
    _executed_tasks.clear()
    queue2 = DelayQueue(persist_path=PERSIST_FILE2)
    queue2.register_callback("tracker", tracking_callback)
    restored = queue2.load_persistent()
    print(f"[{time.strftime('%H:%M:%S')}] 重启后恢复任务数: {restored}")
    print(f"[{time.strftime('%H:%M:%S')}] (预期 = 0，因为 WILL_RUN_2 已在上轮执行完毕；SHOULD_CANCEL_DISK 已被取消不写入)")
    queue2.start()
    time.sleep(2)
    queue2.stop()
    if _executed_tasks:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ 异常 - 重启后又执行了: {sorted(_executed_tasks)}")
    else:
        print(f"[{time.strftime('%H:%M:%S')}] ✅ 重启后无额外任务执行 - 持久化取消正常")

    if os.path.exists(PERSIST_FILE2):
        os.remove(PERSIST_FILE2)


if __name__ == "__main__":
    demo_basic_usage()
    demo_cancel_feature()
    print()
    demo_persistent_restart()

    if os.path.exists(PERSIST_FILE):
        os.remove(PERSIST_FILE)
